using System.Collections.Concurrent;
using Npgsql;
using NpgsqlTypes;
using TaskHorizon.Worker.Persistence;

namespace TaskHorizon.Worker.Ingest;

public sealed class FencingException(int partition, long expected) : Exception($"Partition {partition} checkpoint is not at the expected offset {expected}; another writer owns it.");

public sealed class AggregateStore(NpgsqlDataSource dataSource, IIngestFaults faults)
{
    private const int RowsPerStatement = 20_000;

    private const string UpsertSql = """
        INSERT INTO agg_minute (device_id, metric, bucket, cnt, sum, min, max, sumsq)
        SELECT t.d, t.m, to_timestamp(t.b * 60), t.c, t.s, t.mn, t.mx, t.sq
        FROM unnest(@d, @m, @b, @c, @s, @mn, @mx, @sq) AS t(d, m, b, c, s, mn, mx, sq)
        ON CONFLICT (device_id, metric, bucket) DO UPDATE SET
            cnt   = agg_minute.cnt + EXCLUDED.cnt,
            sum   = agg_minute.sum + EXCLUDED.sum,
            min   = LEAST(agg_minute.min, EXCLUDED.min),
            max   = GREATEST(agg_minute.max, EXCLUDED.max),
            sumsq = agg_minute.sumsq + EXCLUDED.sumsq
        """;

    private readonly ConcurrentDictionary<DateOnly, bool> _knownDays = new();

    public async Task<long> GetCheckpointAsync(int partition, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand("SELECT next_offset FROM partition_checkpoints WHERE partition = @p");
        cmd.Parameters.AddWithValue("p", partition);
        return await cmd.ExecuteScalarAsync(ct) is long v ? v : 0;
    }

    public async Task EnsureDaysAsync(IEnumerable<DateOnly> days, CancellationToken ct)
    {
        foreach (var day in days.Where(d => !_knownDays.ContainsKey(d)))
        {
            await EnsureDayAsync(day, ct);
            _knownDays[day] = true;
        }
    }

    public async Task EnsureDayAsync(DateOnly day, CancellationToken ct)
    {
        var name = $"agg_minute_{day:yyyyMMdd}";
        var from = day.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);
        var to = from.AddDays(1);
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using (var lockCmd = new NpgsqlCommand("SELECT pg_advisory_xact_lock(@ns, @key)", conn, tx))
        {
            lockCmd.Parameters.AddWithValue("ns", Schema.AdvisoryNamespace);
            lockCmd.Parameters.AddWithValue("key", Schema.PartitionDdlLockKey);
            await lockCmd.ExecuteNonQueryAsync(ct);
        }

        await using (var ddl = new NpgsqlCommand(
            $"CREATE TABLE IF NOT EXISTS {name} PARTITION OF agg_minute FOR VALUES FROM ('{from:yyyy-MM-dd HH:mm:ss}+00') TO ('{to:yyyy-MM-dd HH:mm:ss}+00')",
            conn,
            tx))
        {
            await ddl.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
    }

    public async Task CommitAsync(int partition, long expectedOffset, long newOffset, BatchAggregator batch, CancellationToken ct)
    {
        var rows = batch.SortedRows();
        await EnsureDaysAsync(rows.Select(r => DateOnly.FromDateTime(DateTime.UnixEpoch.AddMinutes(r.Key.Minute))).Distinct(), ct);

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        await using (var cas = new NpgsqlCommand(
            "UPDATE partition_checkpoints SET next_offset = @new, updated_at = now() WHERE partition = @p AND next_offset = @expected",
            conn,
            tx))
        {
            cas.Parameters.AddWithValue("new", newOffset);
            cas.Parameters.AddWithValue("p", partition);
            cas.Parameters.AddWithValue("expected", expectedOffset);
            if (await cas.ExecuteNonQueryAsync(ct) != 1)
            {
                throw new FencingException(partition, expectedOffset);
            }
        }

        for (var start = 0; start < rows.Length; start += RowsPerStatement)
        {
            var count = Math.Min(RowsPerStatement, rows.Length - start);
            var d = new int[count];
            var m = new short[count];
            var b = new long[count];
            var c = new long[count];
            var s = new long[count];
            var mn = new int[count];
            var mx = new int[count];
            var sq = new long[count];
            for (var i = 0; i < count; i++)
            {
                var (key, agg) = rows[start + i];
                d[i] = key.DeviceId;
                m[i] = key.Metric;
                b[i] = key.Minute;
                c[i] = agg.Count;
                s[i] = agg.Sum;
                mn[i] = agg.Min;
                mx[i] = agg.Max;
                sq[i] = agg.SumSq;
            }

            await using var upsert = new NpgsqlCommand(UpsertSql, conn, tx);
            upsert.Parameters.Add(new NpgsqlParameter("d", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = d });
            upsert.Parameters.Add(new NpgsqlParameter("m", NpgsqlDbType.Array | NpgsqlDbType.Smallint) { Value = m });
            upsert.Parameters.Add(new NpgsqlParameter("b", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = b });
            upsert.Parameters.Add(new NpgsqlParameter("c", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = c });
            upsert.Parameters.Add(new NpgsqlParameter("s", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = s });
            upsert.Parameters.Add(new NpgsqlParameter("mn", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = mn });
            upsert.Parameters.Add(new NpgsqlParameter("mx", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = mx });
            upsert.Parameters.Add(new NpgsqlParameter("sq", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = sq });
            await upsert.ExecuteNonQueryAsync(ct);
        }

        faults.Hit("mid-commit");
        await tx.CommitAsync(ct);
    }
}

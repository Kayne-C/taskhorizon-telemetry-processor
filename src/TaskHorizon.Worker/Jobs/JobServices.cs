using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace TaskHorizon.Worker.Jobs;

public sealed class JobStats
{
    private long _rollups;
    private long _alerts;
    private long _maintenance;

    public long Rollups => Interlocked.Read(ref _rollups);
    public long Alerts => Interlocked.Read(ref _alerts);
    public long Maintenance => Interlocked.Read(ref _maintenance);

    public void RollupRan() => Interlocked.Increment(ref _rollups);
    public void AlertRan() => Interlocked.Increment(ref _alerts);
    public void MaintenanceRan() => Interlocked.Increment(ref _maintenance);
}

public sealed record MaintenanceResult(int PartitionsCreated, int PartitionsDropped, long HourRowsDeleted);

public sealed class RollupService(NpgsqlDataSource dataSource, IOptions<JobOptions> options)
{
    private const string Sql = """
        INSERT INTO agg_hour (device_id, metric, hour, cnt, sum, min, max, sumsq)
        SELECT device_id, metric, date_trunc('hour', bucket), SUM(cnt)::bigint, SUM(sum)::bigint, MIN(min), MAX(max), SUM(sumsq)::bigint
        FROM agg_minute
        WHERE bucket >= date_trunc('hour', (SELECT max(bucket) FROM agg_minute)) - make_interval(hours => @lookback)
        GROUP BY device_id, metric, date_trunc('hour', bucket)
        ON CONFLICT (device_id, metric, hour) DO UPDATE SET
            cnt = EXCLUDED.cnt, sum = EXCLUDED.sum, min = EXCLUDED.min, max = EXCLUDED.max, sumsq = EXCLUDED.sumsq
        """;

    public async Task<int> RunAsync(CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(Sql);
        cmd.Parameters.AddWithValue("lookback", options.Value.RollupLookbackHours);
        cmd.CommandTimeout = 300;
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<int> RunAllAsync(CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(
            """
            INSERT INTO agg_hour (device_id, metric, hour, cnt, sum, min, max, sumsq)
            SELECT device_id, metric, date_trunc('hour', bucket), SUM(cnt)::bigint, SUM(sum)::bigint, MIN(min), MAX(max), SUM(sumsq)::bigint
            FROM agg_minute
            GROUP BY device_id, metric, date_trunc('hour', bucket)
            ON CONFLICT (device_id, metric, hour) DO UPDATE SET
                cnt = EXCLUDED.cnt, sum = EXCLUDED.sum, min = EXCLUDED.min, max = EXCLUDED.max, sumsq = EXCLUDED.sumsq
            """);
        cmd.CommandTimeout = 600;
        return await cmd.ExecuteNonQueryAsync(ct);
    }
}

public sealed class AlertService(NpgsqlDataSource dataSource, IOptions<JobOptions> options)
{
    private const string Sql = """
        INSERT INTO alerts (device_id, metric, bucket, kind, value, threshold)
        SELECT a.device_id, a.metric, a.bucket, 'max_exceeded', a.max, t.threshold
        FROM agg_minute a
        JOIN unnest(@metrics, @thresholds) AS t(metric, threshold) ON t.metric = a.metric
        WHERE a.bucket >= (SELECT max(bucket) FROM agg_minute) - make_interval(mins => @lookback)
          AND a.max > t.threshold
        ON CONFLICT (device_id, metric, bucket, kind) DO NOTHING
        """;

    public async Task<int> RunAsync(CancellationToken ct)
    {
        var thresholds = options.Value.MaxThresholdsCenti;
        if (thresholds.Count == 0)
        {
            return 0;
        }

        await using var cmd = dataSource.CreateCommand(Sql);
        cmd.Parameters.Add(new NpgsqlParameter("metrics", NpgsqlDbType.Array | NpgsqlDbType.Smallint) { Value = thresholds.Keys.Select(k => (short)k).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter("thresholds", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = thresholds.Values.ToArray() });
        cmd.Parameters.AddWithValue("lookback", options.Value.AlertLookbackMinutes);
        cmd.CommandTimeout = 300;
        return await cmd.ExecuteNonQueryAsync(ct);
    }
}

public sealed class MaintenanceService(NpgsqlDataSource dataSource, IOptions<JobOptions> options, Ingest.AggregateStore store)
{
    public async Task<MaintenanceResult> RunAsync(CancellationToken ct)
    {
        var o = options.Value;
        DateTime? newest;
        await using (var cmd = dataSource.CreateCommand("SELECT max(bucket) FROM agg_minute"))
        {
            newest = await cmd.ExecuteScalarAsync(ct) as DateTime?;
        }

        if (newest is null)
        {
            return new MaintenanceResult(0, 0, 0);
        }

        var today = DateOnly.FromDateTime(newest.Value);
        var existing = await ListPartitionsAsync(ct);

        var created = 0;
        for (var i = 0; i <= o.FuturePartitionDays; i++)
        {
            var day = today.AddDays(i);
            if (!existing.ContainsKey(day))
            {
                await store.EnsureDayAsync(day, ct);
                created++;
            }
        }

        var cutoff = today.AddDays(-o.MinuteRetentionDays);
        var dropped = 0;
        foreach (var (day, name) in existing.Where(kv => kv.Key < cutoff))
        {
            await using var drop = dataSource.CreateCommand($"DROP TABLE IF EXISTS {name}");
            await drop.ExecuteNonQueryAsync(ct);
            dropped++;
        }

        await using var del = dataSource.CreateCommand("DELETE FROM agg_hour WHERE hour < @cutoff");
        del.Parameters.AddWithValue("cutoff", NpgsqlDbType.TimestampTz, newest.Value.AddDays(-o.HourRetentionDays));
        var deleted = await del.ExecuteNonQueryAsync(ct);
        return new MaintenanceResult(created, dropped, deleted);
    }

    private async Task<Dictionary<DateOnly, string>> ListPartitionsAsync(CancellationToken ct)
    {
        var result = new Dictionary<DateOnly, string>();
        await using var cmd = dataSource.CreateCommand(
            "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = 'agg_minute'");
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var name = reader.GetString(0);
            if (name.Length == "agg_minute_".Length + 8 && DateOnly.TryParseExact(name["agg_minute_".Length..], "yyyyMMdd", out var day))
            {
                result[day] = name;
            }
        }

        return result;
    }
}

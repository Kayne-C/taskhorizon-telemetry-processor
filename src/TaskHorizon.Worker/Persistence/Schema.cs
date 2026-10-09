using Microsoft.Extensions.Options;
using Npgsql;

namespace TaskHorizon.Worker.Persistence;

public static class Schema
{
    public const int AdvisoryNamespace = 0x7468;
    public const int SchemaLockKey = -1;
    public const int PartitionDdlLockKey = -2;

    private const string Core = """
        CREATE TABLE IF NOT EXISTS partition_checkpoints (
            partition   int PRIMARY KEY,
            next_offset bigint NOT NULL,
            updated_at  timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS agg_minute (
            device_id int NOT NULL,
            metric    smallint NOT NULL,
            bucket    timestamptz NOT NULL,
            cnt       bigint NOT NULL,
            sum       bigint NOT NULL,
            min       int NOT NULL,
            max       int NOT NULL,
            sumsq     bigint NOT NULL,
            PRIMARY KEY (device_id, metric, bucket)
        ) PARTITION BY RANGE (bucket);

        CREATE INDEX IF NOT EXISTS agg_minute_bucket_idx ON agg_minute (bucket);

        CREATE TABLE IF NOT EXISTS agg_hour (
            device_id int NOT NULL,
            metric    smallint NOT NULL,
            hour      timestamptz NOT NULL,
            cnt       bigint NOT NULL,
            sum       bigint NOT NULL,
            min       int NOT NULL,
            max       int NOT NULL,
            sumsq     bigint NOT NULL,
            PRIMARY KEY (device_id, metric, hour)
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id         bigserial PRIMARY KEY,
            device_id  int NOT NULL,
            metric     smallint NOT NULL,
            bucket     timestamptz NOT NULL,
            kind       text NOT NULL,
            value      int NOT NULL,
            threshold  int NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (device_id, metric, bucket, kind)
        );
        """;

    public static string QuartzDdl()
    {
        using var stream = typeof(Schema).Assembly.GetManifestResourceStream("TaskHorizon.Worker.Persistence.quartz_postgres.sql")
                           ?? throw new InvalidOperationException("Embedded Quartz DDL not found.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    public static async Task EnsureAsync(NpgsqlDataSource dataSource, int partitions, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);

        if (!await IsInstalledAsync(conn, null, ct))
        {
            await using var tx = await conn.BeginTransactionAsync(ct);
            await using (var lockCmd = new NpgsqlCommand("SELECT pg_advisory_xact_lock(@ns, @key)", conn, tx))
            {
                lockCmd.Parameters.AddWithValue("ns", AdvisoryNamespace);
                lockCmd.Parameters.AddWithValue("key", SchemaLockKey);
                await lockCmd.ExecuteNonQueryAsync(ct);
            }

            if (!await IsInstalledAsync(conn, tx, ct))
            {
                await using (var core = new NpgsqlCommand(Core, conn, tx))
                {
                    await core.ExecuteNonQueryAsync(ct);
                }

                await using (var quartz = new NpgsqlCommand(QuartzDdl(), conn, tx))
                {
                    await quartz.ExecuteNonQueryAsync(ct);
                }

                await using var marker = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS schema_info (version int NOT NULL)", conn, tx);
                await marker.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);
        }

        await using var count = new NpgsqlCommand("SELECT count(*) FROM partition_checkpoints", conn);
        if (await count.ExecuteScalarAsync(ct) is long existing && existing < partitions)
        {
            await using var seed = new NpgsqlCommand(
                "INSERT INTO partition_checkpoints (partition, next_offset) SELECT g, 0 FROM generate_series(0, @n - 1) g ON CONFLICT DO NOTHING",
                conn);
            seed.Parameters.AddWithValue("n", partitions);
            await seed.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<bool> IsInstalledAsync(NpgsqlConnection conn, NpgsqlTransaction? tx, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = 'schema_info' AND c.relnamespace = current_schema()::regnamespace)", conn, tx);
        return await cmd.ExecuteScalarAsync(ct) is true;
    }
}

public sealed class SchemaInitializer(NpgsqlDataSource dataSource, IOptions<IngestOptions> ingest) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => Schema.EnsureAsync(dataSource, ingest.Value.Partitions, cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

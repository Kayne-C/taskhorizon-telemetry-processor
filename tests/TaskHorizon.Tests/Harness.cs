using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using TaskHorizon.Worker;
using TaskHorizon.Worker.Ingest;
using TaskHorizon.Worker.Jobs;
using Testcontainers.PostgreSql;
using Xunit;

namespace TaskHorizon.Tests;

public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("public.ecr.aws/docker/library/postgres:17")
        .WithCommand("-c", "max_connections=400")
        .Build();

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    public async Task<string> CreateDatabaseAsync()
    {
        var name = "t_" + Guid.NewGuid().ToString("N");
        await using (var admin = new NpgsqlConnection(_container.GetConnectionString()))
        {
            await admin.OpenAsync();
            await using var cmd = new NpgsqlCommand($"CREATE DATABASE {name}", admin);
            await cmd.ExecuteNonQueryAsync();
        }

        return new NpgsqlConnectionStringBuilder(_container.GetConnectionString()) { Database = name, MaxPoolSize = 40 }.ConnectionString;
    }
}

[CollectionDefinition("postgres")]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>;

public sealed class ChaosFaults(double beforeCommit = 0, double midCommit = 0, double afterCommit = 0) : IIngestFaults
{
    private int _crashes;

    public int Crashes => Volatile.Read(ref _crashes);

    public void Hit(string point)
    {
        var p = point switch
        {
            "before-commit" => beforeCommit,
            "mid-commit" => midCommit,
            "after-commit" => afterCommit,
            _ => 0,
        };

        if (p > 0 && Random.Shared.NextDouble() < p)
        {
            Interlocked.Increment(ref _crashes);
            throw new InvalidOperationException($"Simulated crash at {point}");
        }
    }
}

public sealed class SlowFaults(int milliseconds) : IIngestFaults
{
    public void Hit(string point)
    {
        if (point == "before-commit")
        {
            Thread.Sleep(milliseconds);
        }
    }
}

public sealed record NodeOptions
{
    public int Partitions { get; init; } = 4;
    public long EventsPerPartition { get; init; } = 40_000;
    public int BatchSize { get; init; } = 7_000;
    public int DevicesPerPartition { get; init; } = 50;
    public bool Ingest { get; init; } = true;
    public bool Jobs { get; init; }
    public int RollupIntervalSeconds { get; init; } = 1;
    public IIngestFaults? Faults { get; init; }
    public string SchedulerName { get; init; } = "test";
    public int RollupLookbackHours { get; init; } = 48;
    public int MinuteRetentionDays { get; init; } = 7;
}

public sealed class Node : IAsyncDisposable
{
    private readonly WebApplication _app;

    private Node(WebApplication app, NodeOptions options)
    {
        _app = app;
        Options = options;
    }

    public NodeOptions Options { get; }
    public IServiceProvider Services => _app.Services;
    public IngestEngine Engine => _app.Services.GetRequiredService<IngestEngine>();
    public IngestStats Stats => _app.Services.GetRequiredService<IngestStats>();
    public JobStats Jobs => _app.Services.GetRequiredService<JobStats>();
    public AggregateStore Store => _app.Services.GetRequiredService<AggregateStore>();
    public NpgsqlDataSource DataSource => _app.Services.GetRequiredService<NpgsqlDataSource>();
    public string Address => _app.Urls.First();

    public static async Task<Node> StartAsync(string connectionString, NodeOptions? options = null)
    {
        options ??= new NodeOptions();
        var app = TaskHorizonHost.Build(
            [],
            b =>
            {
                b.Logging.ClearProviders();
                b.Logging.AddConsole();
                b.Logging.SetMinimumLevel(LogLevel.Error);
                b.WebHost.UseUrls("http://127.0.0.1:0");
                b.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Db"] = connectionString,
                    ["TaskHorizon:Ingest:Enabled"] = options.Ingest.ToString(),
                    ["TaskHorizon:Ingest:Partitions"] = options.Partitions.ToString(),
                    ["TaskHorizon:Ingest:EventsPerPartition"] = options.EventsPerPartition.ToString(),
                    ["TaskHorizon:Ingest:BatchSize"] = options.BatchSize.ToString(),
                    ["TaskHorizon:Ingest:DevicesPerPartition"] = options.DevicesPerPartition.ToString(),
                    ["TaskHorizon:Ingest:RestartBackoffMs"] = "20",
                    ["TaskHorizon:Ingest:LeaseRetryMs"] = "100",
                    ["TaskHorizon:Jobs:Enabled"] = options.Jobs.ToString(),
                    ["TaskHorizon:Jobs:SchedulerName"] = options.SchedulerName,
                    ["TaskHorizon:Jobs:RollupIntervalSeconds"] = options.RollupIntervalSeconds.ToString(),
                    ["TaskHorizon:Jobs:AlertIntervalSeconds"] = "3600",
                    ["TaskHorizon:Jobs:MaintenanceIntervalSeconds"] = "3600",
                    ["TaskHorizon:Jobs:RollupLookbackHours"] = options.RollupLookbackHours.ToString(),
                    ["TaskHorizon:Jobs:MinuteRetentionDays"] = options.MinuteRetentionDays.ToString(),
                    ["TaskHorizon:Jobs:AlertLookbackMinutes"] = "100000",
                });
            },
            services =>
            {
                if (options.Faults is not null)
                {
                    services.RemoveAll<IIngestFaults>();
                    services.AddSingleton(options.Faults);
                }
            });

        await app.StartAsync();
        return new Node(app, options);
    }

    public SyntheticSource Source()
    {
        var o = _app.Services.GetRequiredService<IOptions<IngestOptions>>();
        return new SyntheticSource(o);
    }

    public async Task StopAsync() => await _app.StopAsync();

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
        NpgsqlConnection.ClearAllPools();
    }
}

public readonly record struct Row(long Count, long Sum, int Min, int Max, long SumSq);

public static class Truth
{
    public static Dictionary<(int Device, int Metric, long Minute), Row> Expected(SyntheticSource source)
    {
        var result = new Dictionary<(int, int, long), Row>();
        for (var p = 0; p < source.Partitions; p++)
        {
            for (long o = 0; o < source.EndOffset; o++)
            {
                var e = source.EventAt(p, o);
                var key = (e.DeviceId, (int)e.Metric, e.TimestampMs / 60_000);
                result[key] = result.TryGetValue(key, out var r)
                    ? new Row(r.Count + 1, r.Sum + e.Value, Math.Min(r.Min, e.Value), Math.Max(r.Max, e.Value), r.SumSq + (long)e.Value * e.Value)
                    : new Row(1, e.Value, e.Value, e.Value, (long)e.Value * e.Value);
            }
        }

        return result;
    }

    public static async Task<Dictionary<(int Device, int Metric, long Minute), Row>> ActualMinutesAsync(NpgsqlDataSource ds)
    {
        var result = new Dictionary<(int, int, long), Row>();
        await using var cmd = ds.CreateCommand("SELECT device_id, metric, (extract(epoch from bucket) / 60)::bigint, cnt, sum, min, max, sumsq FROM agg_minute");
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            result[(r.GetInt32(0), r.GetInt16(1), r.GetInt64(2))] = new Row(r.GetInt64(3), r.GetInt64(4), r.GetInt32(5), r.GetInt32(6), r.GetInt64(7));
        }

        return result;
    }

    public static async Task<Dictionary<(int Device, int Metric, long Hour), Row>> ActualHoursAsync(NpgsqlDataSource ds)
    {
        var result = new Dictionary<(int, int, long), Row>();
        await using var cmd = ds.CreateCommand("SELECT device_id, metric, (extract(epoch from hour) / 3600)::bigint, cnt, sum, min, max, sumsq FROM agg_hour");
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            result[(r.GetInt32(0), r.GetInt16(1), r.GetInt64(2))] = new Row(r.GetInt64(3), r.GetInt64(4), r.GetInt32(5), r.GetInt32(6), r.GetInt64(7));
        }

        return result;
    }

    public static async Task<long> ScalarAsync(NpgsqlDataSource ds, string sql)
    {
        await using var cmd = ds.CreateCommand(sql);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }
}

using Microsoft.Extensions.Options;
using Npgsql;
using Quartz;
using TaskHorizon.Worker.Ingest;
using TaskHorizon.Worker.Jobs;
using TaskHorizon.Worker.Persistence;

namespace TaskHorizon.Worker;

public static class TaskHorizonHost
{
    public static WebApplication Build(string[] args, Action<WebApplicationBuilder>? configure = null, Action<IServiceCollection>? overrides = null)
    {
        var builder = WebApplication.CreateBuilder(args);
        configure?.Invoke(builder);

        var config = builder.Configuration;
        var connectionString = config.GetConnectionString("Db") ?? throw new InvalidOperationException("ConnectionStrings:Db is required.");
        var jobs = config.GetSection(JobOptions.Section).Get<JobOptions>() ?? new JobOptions();

        builder.Services.Configure<IngestOptions>(config.GetSection(IngestOptions.Section));
        builder.Services.Configure<JobOptions>(config.GetSection(JobOptions.Section));

        builder.Services.AddSingleton(new NpgsqlDataSourceBuilder(WithPool(connectionString)).Build());
        builder.Services.AddSingleton<IEventSource, SyntheticSource>();
        builder.Services.AddSingleton<AggregateStore>();
        builder.Services.AddSingleton<IngestStats>();
        builder.Services.AddSingleton<IIngestFaults, NoIngestFaults>();
        builder.Services.AddSingleton<JobStats>();
        builder.Services.AddSingleton<RollupService>();
        builder.Services.AddSingleton<AlertService>();
        builder.Services.AddSingleton<MaintenanceService>();

        builder.Services.AddHostedService<SchemaInitializer>();

        if (jobs.Enabled)
        {
            builder.Services.AddQuartz(jobs.SchedulerName, q =>
            {
                q.ConfigureScheduler(o => o.GenerateInstanceId = true);
                q.UsePersistentStore(store =>
                {
                    store.UsePostgres(WithPool(connectionString));
                    store.UseSystemTextJsonSerializer();
                    store.UseClustering(c =>
                    {
                        c.CheckinInterval = TimeSpan.FromSeconds(5);
                        c.CheckinMisfireThreshold = TimeSpan.FromSeconds(10);
                    });
                });

                void Schedule<T>(string name, int seconds)
                    where T : IJob
                {
                    q.ScheduleJob<T>(
                        t => t
                            .WithIdentity(name + "-trigger", "taskhorizon")
                            .WithSchedule(SimpleScheduleBuilder.Create()
                                .WithInterval(TimeSpan.FromSeconds(seconds))
                                .RepeatForever()
                                .WithMisfireInstruction(SimpleTriggerMisfireInstruction.NextWithRemainingCount)),
                        j => j.WithIdentity(name, "taskhorizon").StoreDurably());
                }

                Schedule<RollupJob>("rollup", jobs.RollupIntervalSeconds);
                Schedule<AlertJob>("alerts", jobs.AlertIntervalSeconds);
                Schedule<MaintenanceJob>("maintenance", jobs.MaintenanceIntervalSeconds);
            });
            builder.Services.AddQuartzHostedService(o => o.WaitForJobsToComplete = true);
        }

        builder.Services.AddSingleton<IngestEngine>();
        builder.Services.AddHostedService(sp => sp.GetRequiredService<IngestEngine>());
        builder.Services.AddHealthChecks();

        overrides?.Invoke(builder.Services);

        var app = builder.Build();
        MapApi(app);
        return app;
    }

    private static string WithPool(string connectionString)
    {
        var b = new NpgsqlConnectionStringBuilder(connectionString);
        if (b.Timezone is null)
        {
            b.Timezone = "UTC";
        }

        b.GssEncryptionMode = GssEncryptionMode.Disable;

        if (b.MaxPoolSize < 40)
        {
            b.MaxPoolSize = 40;
        }

        return b.ConnectionString;
    }

    private static void MapApi(WebApplication app)
    {
        app.MapHealthChecks("/healthz");

        app.MapGet("/stats", async (IngestStats stats, AggregateStore store, IOptions<IngestOptions> options, CancellationToken ct) =>
        {
            var checkpoints = new List<long>();
            for (var p = 0; p < options.Value.Partitions; p++)
            {
                checkpoints.Add(await store.GetCheckpointAsync(p, ct));
            }

            return new
            {
                stats.Events,
                stats.Batches,
                stats.Restarts,
                eventsPerSecond = Math.Round(stats.EventsPerSecond),
                averageCommitMs = Math.Round(stats.AverageCommitMs, 1),
                checkpoints,
                totalCommitted = checkpoints.Sum(),
                target = options.Value.EventsPerPartition * options.Value.Partitions,
            };
        });

        app.MapGet("/devices/{id:int}/minutes", async (int id, byte metric, int? limit, NpgsqlDataSource ds, CancellationToken ct) =>
        {
            await using var cmd = ds.CreateCommand(
                "SELECT bucket, cnt, sum, min, max, sumsq FROM agg_minute WHERE device_id = @d AND metric = @m ORDER BY bucket DESC LIMIT @l");
            cmd.Parameters.AddWithValue("d", id);
            cmd.Parameters.AddWithValue("m", (short)metric);
            cmd.Parameters.AddWithValue("l", Math.Clamp(limit ?? 60, 1, 1440));
            var rows = new List<object>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var count = reader.GetInt64(1);
                var sum = reader.GetInt64(2);
                var mean = (double)sum / count;
                var variance = (double)reader.GetInt64(5) / count - mean * mean;
                rows.Add(new { bucket = reader.GetDateTime(0), count, average = mean / 100.0, min = reader.GetInt32(3) / 100.0, max = reader.GetInt32(4) / 100.0, stdDev = Math.Sqrt(Math.Max(variance, 0)) / 100.0 });
            }

            return Results.Ok(rows);
        });

        app.MapGet("/alerts", async (int? limit, NpgsqlDataSource ds, CancellationToken ct) =>
        {
            await using var cmd = ds.CreateCommand("SELECT device_id, metric, bucket, kind, value, threshold FROM alerts ORDER BY id DESC LIMIT @l");
            cmd.Parameters.AddWithValue("l", Math.Clamp(limit ?? 50, 1, 500));
            var rows = new List<object>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                rows.Add(new { deviceId = reader.GetInt32(0), metric = reader.GetInt16(1), bucket = reader.GetDateTime(2), kind = reader.GetString(3), value = reader.GetInt32(4) / 100.0, threshold = reader.GetInt32(5) / 100.0 });
            }

            return Results.Ok(rows);
        });
    }
}

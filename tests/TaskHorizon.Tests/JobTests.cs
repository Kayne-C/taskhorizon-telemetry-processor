using Microsoft.Extensions.DependencyInjection;
using NpgsqlTypes;
using TaskHorizon.Worker.Jobs;
using Xunit;

namespace TaskHorizon.Tests;

[Collection("postgres")]
public class JobTests(PostgresFixture pg)
{
    private static async Task<Node> IngestedAsync(PostgresFixture pg, NodeOptions? options = null)
    {
        var node = await Node.StartAsync(await pg.CreateDatabaseAsync(), options ?? new NodeOptions());
        await node.Engine.Completion.WaitAsync(TimeSpan.FromSeconds(120));
        return node;
    }

    [Fact]
    public async Task Hourly_rollup_equals_the_sum_of_its_minutes_and_is_idempotent()
    {
        await using var node = await IngestedAsync(pg);
        var rollup = node.Services.GetRequiredService<RollupService>();

        await rollup.RunAsync(default);
        var first = await Truth.ActualHoursAsync(node.DataSource);
        await rollup.RunAsync(default);
        await rollup.RunAsync(default);
        var third = await Truth.ActualHoursAsync(node.DataSource);

        var expected = Truth.Expected(node.Source())
            .GroupBy(kv => (kv.Key.Device, kv.Key.Metric, Hour: kv.Key.Minute / 60))
            .ToDictionary(
                g => g.Key,
                g => new Row(g.Sum(x => x.Value.Count), g.Sum(x => x.Value.Sum), g.Min(x => x.Value.Min), g.Max(x => x.Value.Max), g.Sum(x => x.Value.SumSq)));

        Assert.Equal(expected.Count, first.Count);
        foreach (var (key, row) in expected)
        {
            Assert.Equal(row, first[key]);
        }

        Assert.Equal(first, third);
    }

    [Fact]
    public async Task A_late_event_is_picked_up_by_the_next_rollup()
    {
        await using var node = await IngestedAsync(pg, new NodeOptions { Partitions = 1, EventsPerPartition = 5_000, DevicesPerPartition = 3 });
        var rollup = node.Services.GetRequiredService<RollupService>();
        await rollup.RunAsync(default);
        var before = await Truth.ActualHoursAsync(node.DataSource);
        var key = before.Keys.First();

        await using (var cmd = node.DataSource.CreateCommand(
            "UPDATE agg_minute SET cnt = cnt + 5, sum = sum + 5000, max = 10000 WHERE (device_id, metric, bucket) = (SELECT device_id, metric, bucket FROM agg_minute WHERE device_id = @d AND metric = @m ORDER BY bucket LIMIT 1)"))
        {
            cmd.Parameters.AddWithValue("d", key.Device);
            cmd.Parameters.AddWithValue("m", (short)key.Metric);
            Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
        }

        await rollup.RunAsync(default);
        var after = await Truth.ActualHoursAsync(node.DataSource);

        Assert.True(after.Values.Sum(r => r.Count) == before.Values.Sum(r => r.Count) + 5);
        Assert.Equal(10000, after.Values.Max(r => r.Max));
    }

    [Fact]
    public async Task Alerts_are_raised_once_per_spike_however_often_the_job_runs()
    {
        await using var node = await IngestedAsync(pg, new NodeOptions { EventsPerPartition = 80_000, BatchSize = 10_000 });
        var alerts = node.Services.GetRequiredService<AlertService>();

        var expected = Truth.Expected(node.Source()).Count(kv => kv.Key.Metric == 0 && kv.Value.Max > 7500);
        Assert.True(expected > 0, "the data set should contain spikes");

        var first = await alerts.RunAsync(default);
        var second = await alerts.RunAsync(default);
        var third = await alerts.RunAsync(default);

        Assert.Equal(expected, first);
        Assert.Equal(0, second + third);
        Assert.Equal(expected, await Truth.ScalarAsync(node.DataSource, "SELECT count(*) FROM alerts"));
    }

    [Fact]
    public async Task Maintenance_drops_expired_partitions_creates_upcoming_ones_and_keeps_recent_data()
    {
        await using var node = await IngestedAsync(pg, new NodeOptions { Partitions = 1, EventsPerPartition = 2_000, MinuteRetentionDays = 3 });
        var newest = await Truth.ScalarAsync(node.DataSource, "SELECT (extract(epoch from max(bucket)) / 86400)::bigint FROM agg_minute");
        var today = DateOnly.FromDateTime(DateTime.UnixEpoch.AddDays(newest));

        foreach (var offset in new[] { 10, 4, 2 })
        {
            var day = today.AddDays(-offset);
            await node.Store.EnsureDayAsync(day, default);
            await using var cmd = node.DataSource.CreateCommand("INSERT INTO agg_minute VALUES (9999, 0, @b, 1, 1, 1, 1, 1)");
            cmd.Parameters.AddWithValue("b", NpgsqlDbType.TimestampTz, day.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc).AddHours(1));
            await cmd.ExecuteNonQueryAsync();
        }

        var result = await node.Services.GetRequiredService<MaintenanceService>().RunAsync(default);

        Assert.Equal(2, result.PartitionsDropped);
        Assert.True(result.PartitionsCreated >= 1);
        Assert.Equal(1, await Truth.ScalarAsync(node.DataSource, "SELECT count(*) FROM agg_minute WHERE device_id = 9999"));
        Assert.True(await Truth.ScalarAsync(node.DataSource, "SELECT count(*) FROM agg_minute WHERE device_id <> 9999") > 0);
    }

    [Fact]
    public async Task Scheduled_jobs_run_once_per_tick_across_a_cluster_and_survive_a_node_failure()
    {
        var db = await pg.CreateDatabaseAsync();
        var options = new NodeOptions { Ingest = false, Jobs = true, RollupIntervalSeconds = 1, SchedulerName = "cluster-test" };
        await using var a = await Node.StartAsync(db, options);
        await using var b = await Node.StartAsync(db, options);

        await Task.Delay(TimeSpan.FromSeconds(6));
        var total = a.Jobs.Rollups + b.Jobs.Rollups;

        Assert.InRange(total, 3, 8);

        await a.StopAsync();
        var before = b.Jobs.Rollups;
        var deadline = DateTime.UtcNow.AddSeconds(45);
        while (b.Jobs.Rollups - before < 3 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(250);
        }

        Assert.True(b.Jobs.Rollups - before >= 3, $"survivor ran only {b.Jobs.Rollups - before} times");
    }
}

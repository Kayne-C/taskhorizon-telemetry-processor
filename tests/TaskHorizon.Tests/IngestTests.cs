using Npgsql;
using TaskHorizon.Worker.Ingest;
using Xunit;

namespace TaskHorizon.Tests;

[Collection("postgres")]
public class IngestTests(PostgresFixture pg)
{
    private static async Task AssertMatchesTruthAsync(Node node)
    {
        var expected = Truth.Expected(node.Source());
        var actual = await Truth.ActualMinutesAsync(node.DataSource);

        Assert.Equal(expected.Count, actual.Count);
        foreach (var (key, row) in expected)
        {
            Assert.True(actual.TryGetValue(key, out var got), $"missing {key}");
            Assert.Equal(row, got);
        }
    }

    private static async Task WaitDoneAsync(Node node, int seconds = 120)
        => await node.Engine.Completion.WaitAsync(TimeSpan.FromSeconds(seconds));

    [Fact]
    public async Task Every_aggregate_equals_an_independent_computation_over_the_same_events()
    {
        await using var node = await Node.StartAsync(await pg.CreateDatabaseAsync());

        await WaitDoneAsync(node);

        await AssertMatchesTruthAsync(node);
        Assert.Equal(160_000, await Truth.ScalarAsync(node.DataSource, "SELECT sum(cnt) FROM agg_minute"));
        Assert.Equal(4 * 40_000, await Truth.ScalarAsync(node.DataSource, "SELECT sum(next_offset) FROM partition_checkpoints"));
        Assert.Equal(160_000, node.Stats.Events);
    }

    [Fact]
    public async Task Crashes_before_during_and_after_commit_never_double_count_or_drop_events()
    {
        var faults = new ChaosFaults(beforeCommit: 0.15, midCommit: 0.15, afterCommit: 0.15);
        await using var node = await Node.StartAsync(await pg.CreateDatabaseAsync(), new NodeOptions { Faults = faults, EventsPerPartition = 60_000, BatchSize = 3_000 });

        await WaitDoneAsync(node);

        Assert.True(faults.Crashes >= 10, $"only {faults.Crashes} crashes were injected");
        Assert.True(node.Stats.Restarts >= 10);
        await AssertMatchesTruthAsync(node);
        Assert.Equal(240_000, await Truth.ScalarAsync(node.DataSource, "SELECT sum(cnt) FROM agg_minute"));
    }

    [Fact]
    public async Task Two_nodes_split_the_partitions_and_every_event_is_counted_exactly_once()
    {
        var db = await pg.CreateDatabaseAsync();
        var options = new NodeOptions { Partitions = 8, EventsPerPartition = 150_000, BatchSize = 3_000 };
        var started = await Task.WhenAll(Node.StartAsync(db, options), Node.StartAsync(db, options));
        await using var a = started[0];
        await using var b = started[1];

        await WaitDoneAsync(a);
        await WaitDoneAsync(b);

        await AssertMatchesTruthAsync(a);
        Assert.Equal(8 * 150_000, a.Stats.Events + b.Stats.Events);
        Assert.True(a.Stats.Events > 0 && b.Stats.Events > 0, $"work was not shared: {a.Stats.Events} / {b.Stats.Events}");
    }

    [Fact]
    public async Task When_a_node_dies_midway_the_other_one_takes_over_without_losing_or_repeating_anything()
    {
        var db = await pg.CreateDatabaseAsync();
        var options = new NodeOptions { Partitions = 4, EventsPerPartition = 200_000, BatchSize = 4_000 };
        var first = await Node.StartAsync(db, options);

        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (first.Stats.Events < 120_000 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        await first.StopAsync();
        var processedByFirst = first.Stats.Events;
        Assert.InRange(processedByFirst, 1, 4 * 200_000 - 1);

        await using var second = await Node.StartAsync(db, options);
        await WaitDoneAsync(second);

        await AssertMatchesTruthAsync(second);
        Assert.Equal(4 * 200_000, await Truth.ScalarAsync(second.DataSource, "SELECT sum(cnt) FROM agg_minute"));
        await first.DisposeAsync();
    }

    [Fact]
    public async Task A_stale_writer_is_fenced_off_and_changes_nothing()
    {
        await using var node = await Node.StartAsync(await pg.CreateDatabaseAsync(), new NodeOptions { Ingest = false });
        var batch = new BatchAggregator();
        batch.Add(new TelemetryEvent(1, 0, 60_000, 500));

        await node.Store.CommitAsync(0, 0, 1, batch, default);
        var stale = await Assert.ThrowsAsync<FencingException>(() => node.Store.CommitAsync(0, 0, 1, batch, default));

        Assert.Contains("Partition 0", stale.Message);
        Assert.Equal(1, await Truth.ScalarAsync(node.DataSource, "SELECT sum(cnt) FROM agg_minute"));
        Assert.Equal(1, await Truth.ScalarAsync(node.DataSource, "SELECT next_offset FROM partition_checkpoints WHERE partition = 0"));
    }

    [Fact]
    public async Task A_failed_commit_leaves_neither_aggregates_nor_checkpoint_behind()
    {
        var faults = new ChaosFaults(midCommit: 1.0);
        await using var node = await Node.StartAsync(await pg.CreateDatabaseAsync(), new NodeOptions { Ingest = false, Faults = faults });
        var batch = new BatchAggregator();
        for (var i = 0; i < 100; i++)
        {
            batch.Add(new TelemetryEvent(i % 5, 0, 60_000 + i, 100 + i));
        }

        await Assert.ThrowsAsync<InvalidOperationException>(() => node.Store.CommitAsync(1, 0, 100, batch, default));

        Assert.Equal(0, await Truth.ScalarAsync(node.DataSource, "SELECT count(*) FROM agg_minute"));
        Assert.Equal(0, await Truth.ScalarAsync(node.DataSource, "SELECT next_offset FROM partition_checkpoints WHERE partition = 1"));
    }

    [Fact]
    public async Task Daily_partitions_are_created_on_demand_and_are_safe_to_create_concurrently()
    {
        await using var node = await Node.StartAsync(await pg.CreateDatabaseAsync(), new NodeOptions { Ingest = false });
        var days = Enumerable.Range(0, 6).Select(i => new DateOnly(2026, 1, 1).AddDays(i)).ToArray();

        await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => node.Store.EnsureDaysAsync(days, default)));

        Assert.Equal(6, await Truth.ScalarAsync(node.DataSource, "SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = 'agg_minute'"));
    }

    [Fact]
    public async Task Late_and_out_of_order_events_are_merged_into_the_right_buckets()
    {
        await using var node = await Node.StartAsync(
            await pg.CreateDatabaseAsync(),
            new NodeOptions { Partitions = 2, EventsPerPartition = 30_000, BatchSize = 1_000 });

        await WaitDoneAsync(node);

        var spreadMinutes = (await Truth.ScalarAsync(node.DataSource, "SELECT count(DISTINCT bucket) FROM agg_minute"));
        Assert.True(spreadMinutes >= 3, "jitter should spread events over several minutes");
        await AssertMatchesTruthAsync(node);
    }
}

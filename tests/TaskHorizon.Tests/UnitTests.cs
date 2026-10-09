using Microsoft.Extensions.Options;
using TaskHorizon.Worker;
using TaskHorizon.Worker.Ingest;
using Xunit;

namespace TaskHorizon.Tests;

public class SourceTests
{
    private static SyntheticSource Create(int partitions = 4, long perPartition = 10_000)
        => new(Options.Create(new IngestOptions { Partitions = partitions, EventsPerPartition = perPartition, DevicesPerPartition = 50 }));

    [Fact]
    public void Events_are_a_pure_function_of_partition_and_offset()
    {
        var a = Create();
        var b = Create();

        for (long o = 0; o < 2_000; o++)
        {
            Assert.Equal(a.EventAt(2, o), b.EventAt(2, o));
        }

        Assert.NotEqual(a.EventAt(0, 5), a.EventAt(1, 5));
    }

    [Fact]
    public void Reading_in_pieces_yields_the_same_stream_as_reading_at_once()
    {
        var source = Create();
        var whole = new TelemetryEvent[10_000];
        source.Read(1, 0, 10_000, whole);

        var pieces = new List<TelemetryEvent>();
        var buffer = new TelemetryEvent[777];
        for (long offset = 0; offset < 10_000;)
        {
            var n = source.Read(1, offset, 777, buffer);
            pieces.AddRange(buffer.Take(n));
            offset += n;
        }

        Assert.Equal(whole, pieces);
    }

    [Fact]
    public void A_device_only_ever_appears_in_its_own_partition_and_values_stay_in_range()
    {
        var source = Create();

        for (var p = 0; p < 4; p++)
        {
            for (long o = 0; o < 3_000; o++)
            {
                var e = source.EventAt(p, o);
                Assert.Equal(p, e.DeviceId % 4);
                Assert.InRange(e.Value, 0, 10_000);
                Assert.InRange(e.Metric, (byte)0, (byte)3);
            }
        }
    }

    [Fact]
    public void Reading_past_the_end_returns_only_what_exists()
    {
        var source = Create(perPartition: 100);
        var buffer = new TelemetryEvent[64];

        Assert.Equal(64, source.Read(0, 0, 64, buffer));
        Assert.Equal(36, source.Read(0, 64, 64, buffer));
        Assert.Equal(0, source.Read(0, 100, 64, buffer));
    }
}

public class AggregationTests
{
    [Theory]
    [InlineData(0, 0)]
    [InlineData(59_999, 0)]
    [InlineData(60_000, 1)]
    [InlineData(119_999, 1)]
    [InlineData(1_767_225_600_000, 29_453_760)]
    public void Events_land_in_the_right_minute_bucket(long timestampMs, long minute)
    {
        var agg = new BatchAggregator();

        agg.Add(new TelemetryEvent(1, 0, timestampMs, 10));

        Assert.Equal(minute, agg.Map.Keys.Single().Minute);
    }

    [Fact]
    public void Statistics_match_a_naive_computation()
    {
        var values = new[] { 1200, 4500, 4500, 9900, 0, 333 };
        var agg = new BatchAggregator();
        foreach (var v in values)
        {
            agg.Add(new TelemetryEvent(7, 2, 120_000, v));
        }

        var row = agg.Map.Values.Single();
        Assert.Equal(values.Length, row.Count);
        Assert.Equal(values.Sum(), row.Sum);
        Assert.Equal(0, row.Min);
        Assert.Equal(9900, row.Max);
        Assert.Equal(values.Sum(v => (long)v * v), row.SumSq);
    }

    [Fact]
    public void Merging_two_partial_aggregations_equals_aggregating_everything_at_once()
    {
        var source = new SyntheticSource(Options.Create(new IngestOptions { Partitions = 2, EventsPerPartition = 20_000, DevicesPerPartition = 20, JitterMs = 120_000 }));
        var events = Enumerable.Range(0, 20_000).Select(o => source.EventAt(0, o)).ToArray();
        var rng = new Random(7);

        for (var round = 0; round < 50; round++)
        {
            var cut = rng.Next(0, events.Length);
            var whole = new BatchAggregator();
            var left = new BatchAggregator();
            var right = new BatchAggregator();
            foreach (var e in events)
            {
                whole.Add(e);
            }

            foreach (var e in events.Take(cut))
            {
                left.Add(e);
            }

            foreach (var e in events.Skip(cut))
            {
                right.Add(e);
            }

            left.Merge(right);

            Assert.Equal(whole.Events, left.Events);
            Assert.Equal(whole.Map.Count, left.Map.Count);
            foreach (var (key, expected) in whole.Map)
            {
                var actual = left.Map[key];
                Assert.Equal((expected.Count, expected.Sum, expected.Min, expected.Max, expected.SumSq), (actual.Count, actual.Sum, actual.Min, actual.Max, actual.SumSq));
            }
        }
    }

    [Fact]
    public void Sorted_rows_are_in_primary_key_order_so_concurrent_writers_cannot_deadlock()
    {
        var agg = new BatchAggregator();
        var rng = new Random(3);
        for (var i = 0; i < 5_000; i++)
        {
            agg.Add(new TelemetryEvent(rng.Next(0, 100), (byte)rng.Next(0, 4), rng.Next(0, 10_000_000), rng.Next(0, 10_000)));
        }

        var rows = agg.SortedRows();

        for (var i = 1; i < rows.Length; i++)
        {
            Assert.True(rows[i - 1].Key.CompareTo(rows[i].Key) < 0);
        }
    }
}

namespace TaskHorizon.Worker.Ingest;

public readonly record struct TelemetryEvent(int DeviceId, byte Metric, long TimestampMs, int Value);

public readonly record struct AggKey(int DeviceId, byte Metric, long Minute) : IComparable<AggKey>
{
    public int CompareTo(AggKey other)
    {
        var c = DeviceId.CompareTo(other.DeviceId);
        if (c != 0)
        {
            return c;
        }

        c = Metric.CompareTo(other.Metric);
        return c != 0 ? c : Minute.CompareTo(other.Minute);
    }
}

public struct Agg
{
    public long Count;
    public long Sum;
    public int Min;
    public int Max;
    public long SumSq;

    public static Agg Of(int value) => new() { Count = 1, Sum = value, Min = value, Max = value, SumSq = (long)value * value };

    public void Add(int value)
    {
        Count++;
        Sum += value;
        if (value < Min)
        {
            Min = value;
        }

        if (value > Max)
        {
            Max = value;
        }

        SumSq += (long)value * value;
    }

    public void Merge(in Agg other)
    {
        Count += other.Count;
        Sum += other.Sum;
        Min = Math.Min(Min, other.Min);
        Max = Math.Max(Max, other.Max);
        SumSq += other.SumSq;
    }
}

public sealed class BatchAggregator
{
    private readonly Dictionary<AggKey, Agg> _map = new(1 << 14);

    public int Count => _map.Count;

    public long Events { get; private set; }

    public IReadOnlyDictionary<AggKey, Agg> Map => _map;

    public void Add(in TelemetryEvent e)
    {
        var key = new AggKey(e.DeviceId, e.Metric, Math.DivRem(e.TimestampMs, 60_000L, out var rem) - (rem < 0 ? 1 : 0));
        ref var slot = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(_map, key, out var exists);
        if (exists)
        {
            slot.Add(e.Value);
        }
        else
        {
            slot = Agg.Of(e.Value);
        }

        Events++;
    }

    public void Merge(BatchAggregator other)
    {
        foreach (var (key, agg) in other._map)
        {
            ref var slot = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(_map, key, out var exists);
            if (exists)
            {
                slot.Merge(agg);
            }
            else
            {
                slot = agg;
            }
        }

        Events += other.Events;
    }

    public KeyValuePair<AggKey, Agg>[] SortedRows()
    {
        var rows = new KeyValuePair<AggKey, Agg>[_map.Count];
        var i = 0;
        foreach (var kv in _map)
        {
            rows[i++] = kv;
        }

        Array.Sort(rows, static (a, b) => a.Key.CompareTo(b.Key));
        return rows;
    }
}

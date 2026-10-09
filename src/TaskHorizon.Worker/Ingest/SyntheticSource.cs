using Microsoft.Extensions.Options;

namespace TaskHorizon.Worker.Ingest;

public interface IEventSource
{
    int Partitions { get; }

    long EndOffset { get; }

    int Read(int partition, long offset, int max, TelemetryEvent[] buffer);
}

public sealed class SyntheticSource : IEventSource
{
    private static readonly int[] Baselines = [4500, 2000, 6000, 3000];

    private readonly IngestOptions _o;

    public SyntheticSource(IOptions<IngestOptions> options)
    {
        _o = options.Value;
    }

    public int Partitions => _o.Partitions;

    public long EndOffset => _o.EventsPerPartition;

    public int Read(int partition, long offset, int max, TelemetryEvent[] buffer)
    {
        var n = (int)Math.Min(max, Math.Min(buffer.Length, _o.EventsPerPartition - offset));
        for (var i = 0; i < n; i++)
        {
            buffer[i] = EventAt(partition, offset + i);
        }

        return Math.Max(n, 0);
    }

    public TelemetryEvent EventAt(int partition, long offset)
    {
        var state = _o.Seed ^ ((ulong)partition << 48) ^ (ulong)offset * 0x9E3779B97F4A7C15UL;
        var a = Next(ref state);
        var b = Next(ref state);
        var c = Next(ref state);

        var device = partition + _o.Partitions * (int)(a % (ulong)_o.DevicesPerPartition);
        var metric = (byte)(b % 4);
        var jitter = _o.JitterMs == 0 ? 0 : (long)(c % (ulong)(2 * _o.JitterMs + 1)) - _o.JitterMs;
        var timestamp = _o.BaseTimeMs + offset * _o.StepMs + jitter;

        var noise = (int)((a >> 20) % 2001) - 1000;
        var value = Baselines[metric] + noise;
        if ((b >> 12) % 2000 == 0)
        {
            value += 3000;
        }

        return new TelemetryEvent(device, metric, timestamp, Math.Clamp(value, 0, 10_000));
    }

    private static ulong Next(ref ulong state)
    {
        state += 0x9E3779B97F4A7C15UL;
        var z = state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}

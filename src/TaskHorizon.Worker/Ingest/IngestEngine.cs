using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using Npgsql;
using TaskHorizon.Worker.Persistence;

namespace TaskHorizon.Worker.Ingest;

public interface IIngestFaults
{
    void Hit(string point);
}

public sealed class NoIngestFaults : IIngestFaults
{
    public void Hit(string point)
    {
    }
}

public sealed class PartitionLease : IAsyncDisposable
{
    private readonly NpgsqlConnection _connection;
    private readonly int _partition;

    private PartitionLease(NpgsqlConnection connection, int partition)
    {
        _connection = connection;
        _partition = partition;
    }

    public static async Task<PartitionLease?> TryAcquireAsync(NpgsqlDataSource dataSource, int partition, CancellationToken ct)
    {
        var connection = await dataSource.OpenConnectionAsync(ct);
        try
        {
            await using var cmd = new NpgsqlCommand("SELECT pg_try_advisory_lock(@ns, @key)", connection);
            cmd.Parameters.AddWithValue("ns", Schema.AdvisoryNamespace);
            cmd.Parameters.AddWithValue("key", partition);
            if (await cmd.ExecuteScalarAsync(ct) is true)
            {
                return new PartitionLease(connection, partition);
            }
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }

        await connection.DisposeAsync();
        return null;
    }

    public async Task<bool> IsAliveAsync(CancellationToken ct)
    {
        try
        {
            await using var cmd = new NpgsqlCommand(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND classid = @ns AND objid = @key AND granted",
                _connection);
            cmd.Parameters.AddWithValue("ns", Schema.AdvisoryNamespace);
            cmd.Parameters.AddWithValue("key", _partition);
            return await cmd.ExecuteScalarAsync(ct) is long n && n > 0;
        }
        catch (Exception) when (!ct.IsCancellationRequested)
        {
            return false;
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await using var cmd = new NpgsqlCommand("SELECT pg_advisory_unlock_all()", _connection);
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception)
        {
        }

        await _connection.DisposeAsync();
    }
}

public sealed class IngestStats
{
    private long _events;
    private long _batches;
    private long _restarts;
    private long _commitTicks;
    private long _startTimestamp;
    private long _lastTimestamp;
    private long[] _offsets = [];

    public void Start(int partitions)
    {
        _offsets = new long[partitions];
        Volatile.Write(ref _startTimestamp, Stopwatch.GetTimestamp());
    }

    public void Batch(int partition, long newOffset, int events, long commitTicks)
    {
        Interlocked.Add(ref _events, events);
        Interlocked.Increment(ref _batches);
        Interlocked.Add(ref _commitTicks, commitTicks);
        Volatile.Write(ref _offsets[partition], newOffset);
        Volatile.Write(ref _lastTimestamp, Stopwatch.GetTimestamp());
    }

    public void Restart() => Interlocked.Increment(ref _restarts);

    public long Events => Interlocked.Read(ref _events);
    public long Batches => Interlocked.Read(ref _batches);
    public long Restarts => Interlocked.Read(ref _restarts);

    public double ActiveSeconds
    {
        get
        {
            var start = Volatile.Read(ref _startTimestamp);
            var last = Volatile.Read(ref _lastTimestamp);
            return start == 0 || last <= start ? 0 : Stopwatch.GetElapsedTime(start, last).TotalSeconds;
        }
    }

    public double EventsPerSecond => ActiveSeconds <= 0 ? 0 : Events / ActiveSeconds;

    public double AverageCommitMs => Batches == 0 ? 0 : TimeSpan.FromTicks(Interlocked.Read(ref _commitTicks) * TimeSpan.TicksPerSecond / Stopwatch.Frequency).TotalMilliseconds / Batches;

    public long[] Offsets => _offsets.Select(o => Volatile.Read(ref o)).ToArray();
}

public sealed class IngestEngine(
    IEventSource source,
    AggregateStore store,
    NpgsqlDataSource dataSource,
    IOptions<IngestOptions> options,
    IngestStats stats,
    IIngestFaults faults,
    ILogger<IngestEngine> logger) : BackgroundService
{
    private readonly TaskCompletionSource _done = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private SemaphoreSlim? _commitGate;

    public Task Completion => _done.Task;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var o = options.Value;
        if (!o.Enabled)
        {
            _done.TrySetResult();
            return;
        }

        await Task.Yield();
        stats.Start(source.Partitions);
        _commitGate = new SemaphoreSlim(o.MaxConcurrentCommits);
        try
        {
            await Task.WhenAll(Enumerable.Range(0, source.Partitions).Select(p => Task.Run(async () => await RunPartitionAsync(p, stoppingToken), stoppingToken)));
            _done.TrySetResult();
        }
        catch (OperationCanceledException)
        {
            _done.TrySetCanceled(stoppingToken);
        }
    }

    private async Task RunPartitionAsync(int partition, CancellationToken ct)
    {
        var o = options.Value;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (await store.GetCheckpointAsync(partition, ct) >= source.EndOffset)
                {
                    return;
                }

                await using var lease = await PartitionLease.TryAcquireAsync(dataSource, partition, ct);
                if (lease is null)
                {
                    await Task.Delay(o.LeaseRetryMs, ct);
                    continue;
                }

                await ConsumeAsync(partition, lease, ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                stats.Restart();
                logger.LogWarning("Partition {Partition} consumer restarting: {Error}", partition, ex.Message);
                await Task.Delay(o.RestartBackoffMs, ct);
            }
        }
    }

    private async Task ConsumeAsync(int partition, PartitionLease lease, CancellationToken ct)
    {
        var o = options.Value;
        var start = await store.GetCheckpointAsync(partition, ct);
        if (start >= source.EndOffset)
        {
            return;
        }

        var channel = Channel.CreateBounded<(long Offset, int Count, BatchAggregator Batch)>(new BoundedChannelOptions(2) { SingleReader = true, SingleWriter = true });
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var producer = Task.Run(
            async () =>
            {
                try
                {
                    var buffer = new TelemetryEvent[o.BatchSize];
                    for (var offset = start; offset < source.EndOffset;)
                    {
                        var n = source.Read(partition, offset, o.BatchSize, buffer);
                        var batch = new BatchAggregator();
                        for (var i = 0; i < n; i++)
                        {
                            batch.Add(buffer[i]);
                        }

                        await channel.Writer.WriteAsync((offset, n, batch), linked.Token);
                        offset += n;
                    }

                    channel.Writer.TryComplete();
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                }
            },
            linked.Token);

        try
        {
            await foreach (var (offset, count, batch) in channel.Reader.ReadAllAsync(linked.Token))
            {
                faults.Hit("before-commit");
                var started = Stopwatch.GetTimestamp();
                await _commitGate!.WaitAsync(linked.Token);
                try
                {
                    await store.CommitAsync(partition, offset, offset + count, batch, linked.Token);
                }
                finally
                {
                    _commitGate.Release();
                }

                stats.Batch(partition, offset + count, count, Stopwatch.GetTimestamp() - started);
                faults.Hit("after-commit");

                if (!await lease.IsAliveAsync(linked.Token))
                {
                    throw new InvalidOperationException($"Lost the lease of partition {partition}.");
                }
            }
        }
        finally
        {
            await linked.CancelAsync();
            try
            {
                await producer;
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}

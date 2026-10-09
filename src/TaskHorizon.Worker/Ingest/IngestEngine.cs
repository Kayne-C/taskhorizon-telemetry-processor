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

public sealed class NodeMembership : IAsyncDisposable
{
    public const int KeyFloor = 1_000_000;
    public const int KeyCeiling = 1_900_000_000;

    private readonly NpgsqlConnection _connection;

    private NodeMembership(NpgsqlConnection connection, int key)
    {
        _connection = connection;
        Key = key;
    }

    public int Key { get; }

    public static async Task<NodeMembership> JoinAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        var connection = await dataSource.OpenConnectionAsync(ct);
        try
        {
            while (true)
            {
                var key = Random.Shared.Next(KeyFloor, KeyCeiling);
                await using var cmd = new NpgsqlCommand("SELECT pg_try_advisory_lock(@ns, @key)", connection);
                cmd.Parameters.AddWithValue("ns", Schema.AdvisoryNamespace);
                cmd.Parameters.AddWithValue("key", key);
                if (await cmd.ExecuteScalarAsync(ct) is true)
                {
                    return new NodeMembership(connection, key);
                }
            }
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    public async Task<int> LiveNodesAsync(CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT count(DISTINCT objid) FROM pg_locks WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) AND classid = @ns AND objsubid = 2 AND objid >= @floor AND objid < @ceiling AND granted",
            _connection);
        cmd.Parameters.AddWithValue("ns", Schema.AdvisoryNamespace);
        cmd.Parameters.AddWithValue("floor", KeyFloor);
        cmd.Parameters.AddWithValue("ceiling", KeyCeiling);
        return Math.Max(1, Convert.ToInt32(await cmd.ExecuteScalarAsync(ct)));
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
    private NodeMembership? _membership;
    private int _owned;
    private int _fairShare;

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
        _fairShare = source.Partitions;
        try
        {
            _membership = await NodeMembership.JoinAsync(dataSource, stoppingToken);
            using var refreshCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            var refresh = Task.Run(async () => await RefreshFairShareAsync(refreshCts.Token), refreshCts.Token);
            try
            {
                await Task.WhenAll(Enumerable.Range(0, source.Partitions).Select(p => Task.Run(async () => await RunPartitionAsync(p, stoppingToken), stoppingToken)));
            }
            finally
            {
                await refreshCts.CancelAsync();
                try
                {
                    await refresh;
                }
                catch (OperationCanceledException)
                {
                }
            }

            _done.TrySetResult();
        }
        catch (OperationCanceledException)
        {
            _done.TrySetCanceled(stoppingToken);
        }
        finally
        {
            if (_membership is not null)
            {
                await _membership.DisposeAsync();
            }
        }
    }

    public int OwnedPartitions => Volatile.Read(ref _owned);

    private async Task RefreshFairShareAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(250));
        do
        {
            try
            {
                var nodes = await _membership!.LiveNodesAsync(ct);
                Volatile.Write(ref _fairShare, (source.Partitions + nodes - 1) / nodes);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning("Could not refresh the live node count: {Error}", ex.Message);
            }
        }
        while (await timer.WaitForNextTickAsync(ct));
    }

    private bool TryYield()
    {
        var fair = Volatile.Read(ref _fairShare);
        while (true)
        {
            var owned = Volatile.Read(ref _owned);
            if (owned <= fair)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _owned, owned - 1, owned) == owned)
            {
                return true;
            }
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

                if (Volatile.Read(ref _owned) >= Volatile.Read(ref _fairShare))
                {
                    await Task.Delay(o.LeaseRetryMs, ct);
                    continue;
                }

                await using var lease = await PartitionLease.TryAcquireAsync(dataSource, partition, ct);
                if (lease is null)
                {
                    await Task.Delay(o.LeaseRetryMs, ct);
                    continue;
                }

                Interlocked.Increment(ref _owned);
                var released = false;
                try
                {
                    released = await ConsumeAsync(partition, lease, ct);
                }
                finally
                {
                    if (!released)
                    {
                        Interlocked.Decrement(ref _owned);
                    }
                }

                if (released)
                {
                    await Task.Delay(o.LeaseRetryMs, ct);
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                stats.Restart();
                logger.LogWarning("Partition {Partition} consumer restarting: {Error}", partition, ex.Message);
                await Task.Delay(o.RestartBackoffMs, ct);
            }
        }
    }

    private async Task<bool> ConsumeAsync(int partition, PartitionLease lease, CancellationToken ct)
    {
        var o = options.Value;
        var start = await store.GetCheckpointAsync(partition, ct);
        if (start >= source.EndOffset)
        {
            return false;
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

                if (offset + count < source.EndOffset && TryYield())
                {
                    return true;
                }
            }

            return false;
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

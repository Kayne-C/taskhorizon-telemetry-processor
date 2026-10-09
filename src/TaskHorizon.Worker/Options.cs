namespace TaskHorizon.Worker;

public sealed class IngestOptions
{
    public const string Section = "TaskHorizon:Ingest";

    public bool Enabled { get; set; } = true;
    public int Partitions { get; set; } = 8;
    public long EventsPerPartition { get; set; } = 1_000_000;
    public int BatchSize { get; set; } = 50_000;
    public int DevicesPerPartition { get; set; } = 500;
    public long BaseTimeMs { get; set; } = 1_767_225_600_000;
    public int StepMs { get; set; } = 1;
    public int JitterMs { get; set; } = 60_000;
    public ulong Seed { get; set; } = 42;
    public int MaxConcurrentCommits { get; set; } = 4;
    public int RestartBackoffMs { get; set; } = 200;
    public int LeaseRetryMs { get; set; } = 500;
}

public sealed class JobOptions
{
    public const string Section = "TaskHorizon:Jobs";

    public int RollupIntervalSeconds { get; set; } = 30;
    public int RollupLookbackHours { get; set; } = 3;
    public int AlertIntervalSeconds { get; set; } = 15;
    public int AlertLookbackMinutes { get; set; } = 180;
    public int MaintenanceIntervalSeconds { get; set; } = 300;
    public int MinuteRetentionDays { get; set; } = 7;
    public int HourRetentionDays { get; set; } = 365;
    public int FuturePartitionDays { get; set; } = 2;
    public Dictionary<byte, int> MaxThresholdsCenti { get; set; } = new() { [0] = 7500 };
    public bool Enabled { get; set; } = true;
    public string SchedulerName { get; set; } = "TaskHorizon";
}

public sealed class DatabaseOptions
{
    public const string Section = "ConnectionStrings";
}

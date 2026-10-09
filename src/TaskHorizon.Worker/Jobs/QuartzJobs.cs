using Quartz;

namespace TaskHorizon.Worker.Jobs;

[DisallowConcurrentExecution]
public sealed class RollupJob(RollupService service, JobStats stats, ILogger<RollupJob> logger) : IJob
{
    public async ValueTask Execute(IJobExecutionContext context, CancellationToken cancellationToken)
    {
        var rows = await service.RunAsync(cancellationToken);
        stats.RollupRan();
        logger.LogInformation("Rollup upserted {Rows} hourly rows", rows);
    }
}

[DisallowConcurrentExecution]
public sealed class AlertJob(AlertService service, JobStats stats, ILogger<AlertJob> logger) : IJob
{
    public async ValueTask Execute(IJobExecutionContext context, CancellationToken cancellationToken)
    {
        var rows = await service.RunAsync(cancellationToken);
        stats.AlertRan();
        if (rows > 0)
        {
            logger.LogInformation("Raised {Rows} new alerts", rows);
        }
    }
}

[DisallowConcurrentExecution]
public sealed class MaintenanceJob(MaintenanceService service, JobStats stats, ILogger<MaintenanceJob> logger) : IJob
{
    public async ValueTask Execute(IJobExecutionContext context, CancellationToken cancellationToken)
    {
        var result = await service.RunAsync(cancellationToken);
        stats.MaintenanceRan();
        logger.LogInformation("Maintenance: {Result}", result);
    }
}

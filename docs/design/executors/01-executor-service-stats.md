# Scheduled and Durable Executor Service Statistics

|ℹ️ Since: 4.1|
|-------------|

## Background

### Description

Hazelcast has 3 different executor service implementations exposed with 
public api. These three different types of executor services are called
as executor service, scheduled executor service and durable executor 
service. Only executor type which has statistics available 
among these is executor service. Statistics collection is not done for 
scheduled and durable ones.


## Design

In this design, we make same statistics with executor service available 
for the other executor types. These statistics are only available for
monitoring from management center. The statistics we have currently is 
represented with class `LocalExecutorStats`.

```
public interface LocalExecutorStats extends LocalInstanceStats {

    /**
     * Returns the number of pending operations on the executor service.
     *
     * @return the number of pending operations on the executor service
     */
    long getPendingTaskCount();

    /**
     * Returns the number of started operations on the executor service.
     *
     * @return the number of started operations on the executor service
     */
    long getStartedTaskCount();

    /**
     * Returns the number of completed operations on the executor service.
     *
     * @return the number of completed operations on the executor service
     */
    long getCompletedTaskCount();

    /**
     * Returns the number of cancelled operations on the executor service.
     *
     * @return the number of cancelled operations on the executor service
     */
    long getCancelledTaskCount();

    /**
     * Returns the total start latency of operations started.
     *
     * @return the total start latency of operations started
     */
    long getTotalStartLatency();

    /**
     * Returns the total execution time of operations finished.
     *
     * @return the total execution time of operations finished
     */
    long getTotalExecutionLatency();
}
```

On submit of each task, we start to collect these statistics and
with metrics subsystem, we made these available to management center.
`DynamicMetricsProvider` interface is implemented by `DistributedScheduledExecutorService`
and `DistributedDurableExecutorService` for this purpose. Statistics are
available by default, they can be disabled via `ScheduledExecutorConfig` or
`DurableExecutorConfig`.

**NOTE**: Durable executor service has no cancellation capability hence no stats available for it.

### Metrics Prefixes
`scheduledExecutor` and `durableExecutor` prefixes are used for 
scheduled and durable executor services respectively.

### Example Output
```
[name=executorName,unit=ms,metric=scheduledExecutor.creationTime]=1598016899537
[name=executorName,unit=count,metric=scheduledExecutor.pending]=0
[name=executorName,unit=count,metric=scheduledExecutor.started]=1
[name=executorName,unit=count,metric=scheduledExecutor.completed]=0
[name=executorName,unit=count,metric=scheduledExecutor.cancelled]=0
[name=executorName,unit=ms,metric=scheduledExecutor.totalStartLatency]=2
[name=executorName,unit=ms,metric=scheduledExecutor.totalExecutionTime]=0
```
 

 
 
 




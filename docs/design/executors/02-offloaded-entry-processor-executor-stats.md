# Offloaded Entry Processor Executor Statistics

|ℹ️ Since: 4.1|
|-------------|

## Background

### Description

Offloaded entry processors are not executed by regular operation threads.
As its name suggests, they offloaded to a different pool of threads and 
executed by them. But the executor of these offloaded entry processors
has no statistics available.


## Design

In this design, we collect statistics for offloaded entry processor executors. 
We use same `LocalExecutorStats` with other executors in Hazelcast.

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

On submit of each offloadable entry processor, we start to collect the statistics and
with metrics subsystem, we made these available to management center.
In `MapService` we have `DynamicMetricsProvider` interface implementation
and offloaded executor stats are added there too. Statistics are available by default,
 they can be disabled via `MapConfig`.
 

 
 
 




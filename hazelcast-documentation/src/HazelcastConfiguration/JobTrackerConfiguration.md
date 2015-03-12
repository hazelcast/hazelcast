
## MapReduce Jobtracker Configuration

The MapReduce JobTracker configuration sets the behavior of the Hazelcast MapReduce framework. Every JobTracker is capable of running multiple MapReduce jobs at the same time. Therefore, one configuration is a shared resource for all jobs created by the same JobTracker. The configuration gives full control over the expected load behavior and thread counts to be used.


**Declarative:**

```xml
<job-tracker name="default">
   <max-thread-size>0</max-thread-size>
   <queue-size>0</queue-size>
   <retry-count>0</retry-count>
   <chunk-size>1000</chunk-size>
   <communicate-stats>true</communicate-stats>
   <topology-changed-strategy>CANCEL_RUNNING_OPERATION</topology-changed-strategy>
</job-tracker>
```

**Programmatic:**

```java
Config config = new Config();
JobTrackerConfig JTcfg = config.getJobTrackerConfig()
JTcfg.setName( "default" ).setQueueSize( "0" )
         .setChunkSize( "1000" );
```
   

MapReduce JobTracker configuration has the following elements.


- `max-thread-size`: The maximum thread pool size of the JobTracker.
- `queue-size`: The maximum number of tasks that can wait to be processed. A value of 0 means an unbounded queue. Very low numbers can prevent successful execution since the job might not be correctly scheduled or intermediate chunks are lost.
- `retry-count`: Currently not used but reserved for later use where the framework will automatically try to restart / retry operations from an available save point.
- `chunk-size`: Defines the number of emitted values before a chunk is sent to the reducers. If your emitted values are big or you want to better balance your work, you might want to change this to a lower or higher value. A value of 0 means immediate transmission, but remember that low values mean higher traffic costs. A very high value might cause an OutOfMemoryError if the emitted values do not fit into heap memory before
being sent to reducers. To prevent this, you might want to use a combiner to pre-reduce values on mapping nodes.
- `communicate-stats`: Defines if statistics (for example, about processed entries) are transmitted to the job emitter. This might be used to show some kind of progress to a user inside of an UI system, but it produces additional traffic. If not needed, you might want to deactivate this.
- `topology-changed-strategy`: Defines how the MapReduce framework will react on topology changes while executing a job. Currently, only CANCEL_RUNNING_OPERATION is fully supported, which throws an exception to the job emitter (will throw a `com.hazelcast.mapreduce.TopologyChangedException`).


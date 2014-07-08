## Queue

Hazelcast distributed queue is an implementation of `java.util.concurrent.BlockingQueue`.

```java
import com.hazelcast.core.Hazelcast;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import com.hazelcast.config.Config;

Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
BlockingQueue<MyTask> q = hz.getQueue("tasks");
q.put(new MyTask());
MyTask task = q.take();

boolean offered = q.offer(new MyTask(), 10, TimeUnit.SECONDS);
task = q.poll (5, TimeUnit.SECONDS);
if (task != null) {
    //process task
}
```

FIFO ordering will apply to all queue operations cluster wide. User objects (such as `MyTask` in the example above), that are (en/de)queued have to be `Serializable`. By configuring `max-size` for queue, one can obtain a bounded queue.

There is no batching while iterating over Queue, items will be copied to local and iteration will occur locally.

A sample configuration is shown below.

```xml
<hazelcast>
    ...
    <queue name="tasks">
        <!--
            Maximum size of the queue. When queue size reaches the maximum,
            all put operations will get blocked until the queue size
            goes down below the maximum.
            Any integer between 0 and Integer.MAX_VALUE. 0 means Integer.MAX_VALUE. Default is 0.
        -->
        <max-size>10000</max-size>

        <!--
        Number of backups. If 1 is set as the backup-count for example,
        then all entries of the map will be copied to another JVM for
        fail-safety. Valid numbers are 0 (no backup), 1, 2 ... 6.
        Default is 1.
        -->
        <backup-count>1</backup-count>

        <!--
            Number of async backups. 0 means no backup.
        -->
        <async-backup-count>0</async-backup-count>

        <!--
            QueueStore implementation to persist items.
            'binary' property indicates that storing items will be in binary format
            'memory-limit' property enables 'overflow to store' after reaching limit
            'bulk-load' property enables bulk-loading from store
        -->
        <queue-store>
            <class-name>com.hazelcast.QueueStore</class-name>
            <properties>
                <property name="binary">false</property>
                <property name="memory-limit">1000</property>
                <property name="bulk-load">250</property>
            </properties>
        </queue-store>
    </queue>
</hazelcast>
```

### Persistence


Hazelcast allows you to load and store the distributed queue entries from/to a persistent datastore such as relational database via a queue-store. If queue store is enabled, each entry added to queue will also be stored at the configured queue store. When the number of items in queue exceeds the memory limit, items will only persisted to queue store, they will not stored in queue memory. Below are the queue store configuration options:

-   **Binary**:
    By default, Hazelcast stores queue items in serialized form in memory and before inserting into datastore, deserializes them. But if you will not reach the queue store from an external application, you can prefer the items to be inserted in binary form. So you get rid of de-serialization step which is a performance optimization. Binary feature is disabled by default.
    
-   **Memory Limit**:
    This is the number of items after which Hazelcast will just store items to datastore. For example, if memory limit is 1000, then 1001st item will be just put into datastore. This feature is useful when you want to avoid out-of-memory conditions. Default number for memory limit is 1000. If you want to always use memory, you can set it to `Integer.MAX\_VALUE`.
    
-   **Bulk Load**:
    At initialization of queue, items are loaded from QueueStore in bulks. Bulk load is the size of these bulks. By default it is 250.

Below is an example queue store configuration:

```xml
<queue-store>
    <class-name>com.hazelcast.QueueStoreImpl</class-name>
    <properties>
        <property name="binary">false</property>
        <property name="memory-limit">10000</property>
        <property name="bulk-load">500</property>
    </properties>
</queue-store>
```

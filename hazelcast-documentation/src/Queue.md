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


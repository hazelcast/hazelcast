### Configuring Queue

An example declarative configuration is shown below.

```xml
<hazelcast>
  ...
  <queue name="tasks">
    <max-size>10</max-size>
    <backup-count>1</backup-count>
    <async-backup-count>1</async-backup-count>
    <empty-queue-ttl>10</empty-queue-ttl>
  </queue>
</hazelcast>
```

Hazelcast distributed queue has one synchronous backup by default. By having this backup, when a cluster member with a queue goes down, another member having the backups will continue. Therefore, no items are lost. You can define the count of synchronous backups using the `backup-count` element in the declarative configuration. A queue can also have asynchronous backups, you can define the count using the `async-backup-count` element.


The `max-size` element defines the maximum size of the queue. You can use the `empty-queue-ttl` element when you want to purge unused or empty queues after a period of time. If you define a value (time in seconds) for this element, then your queue will be destroyed if it stays empty or unused for the time you give.

<br></br>

***RELATED INFORMATION***

*Please refer to the [Queue Configuration section](#queue-configuration) for a full description of Hazelcast Distributed Queue configuration.*


### Queue Backups

Hazelcast distributed queue has one synchronous backup by default. It is set by the `backup-count` property in declarative configuration. By having this backup, when a cluster member with a queue goes down, another member having the backups will continue. So no items are lost. A queue can also have asynchronous backups using the `async-backup-count` property.

A sample configuration is shown below.

```xml
<hazelcast>
    ...
    <queue name="tasks">
         <max-size>10</max-size>
        <backup-count>1</backup-count>
        <async-backup-count>1</async-backup-count>
     </queue>
</hazelcast>
```


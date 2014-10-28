### Bounded Queue

Hazelcast distributed queue offers a size control, which causes the queue to be a bounded queue. Queue capacity can be set for each cluster member using the `max-size` property in the declarative configuration, as shown below. `max-size` specifies the maximum size of the queue. Once the queue size reaches this value, `put` operations will be blocked until the queue size goes below `max-size`.

Let's set **10** as the maximum size of our sample queue in the Sample Queue Code.


```xml
<hazelcast>
  ...
  <queue name="queue">
    <max-size>10</max-size>
  </queue>
  ...
</hazelcast>
```

When the producer is started, 10 items are put into the queue and then the queue will not allow more `put` operations. When the consumer is started, it means the total size is now 20 and the producer can `put` another 10 items. 

But in this sample code, the producer is 5 times faster than the consumer. For this sample code, it is a good option to start multiple consumers or to empty the members.
  
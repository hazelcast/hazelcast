### Bounded Queue

A bounded queue is a queue with a limited capacity. When the bounded queue is full, no more items can be put into the queue until some items are taken out.

A Hazelcast distributed queue can be turned into a bounded queue by setting the capacity limit using the `max-size` property.

Queue capacity can be set using the `max-size` property in the configuration, as shown below. `max-size` specifies the maximum size of the queue. Once the queue size reaches this value, `put` operations will be blocked until the queue size goes below `max-size`, that happens when a consumer removes items from the queue.

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

When the producer is started, 10 items are put into the queue and then the queue will not allow more `put` operations. When the consumer is started, it will remove items from the queue. This means that the producer can `put` more items into the queue until there are 10 items in the queue again, at which point `put` operation again become blocked.

But in this sample code, the producer is 5 times faster than the consumer. It will effectively always be waiting for the consumer to remove items before it can put more on the queue. For this sample code, if maximum throughput was the goal, it would be a good option to start multiple consumers to prevent the queue from filling up.
  
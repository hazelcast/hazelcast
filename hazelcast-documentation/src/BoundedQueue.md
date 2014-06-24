### Bounded Queue

Hazelcast distributed queue offers a size control to form a bounded queue. Queue capacity can be set for each cluster member using the `max-size` property in the declarative configuration, as shown below. It specifies the maximum size of the queue. Once queue size reaches this value, put operations will be blocked until the size goes down below it.

Let's give **10** as the maximum size of our sample queue above.


```xml
<hazelcast>
    ...
    <queue name="queue">
        <max-size>10</max-size>
     </queue>
     ...
</hazelcast>
```

So, when the producer is started once, 10 items are put into the queue and then it will not allow for any put operation.  Once the consumer is started, it means the total size is now 20 and the producer again starts to put another 10 items. 

But, again, the producer is 5 times faster than the consumer in our sample. So, it is a good option to start multiple consumers or just emptying the members.
  
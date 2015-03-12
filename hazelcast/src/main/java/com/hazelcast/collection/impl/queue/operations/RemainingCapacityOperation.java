package com.hazelcast.collection.impl.queue.operations;


import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;

/**
 * Returns the remaining capacity of the queue based on config max-size
 */
public class RemainingCapacityOperation extends QueueOperation {

    public RemainingCapacityOperation() {
    }

    public RemainingCapacityOperation(String name) {
        super(name);
    }

    @Override
    public void run() {
        QueueContainer queueContainer = getOrCreateContainer();
        response = queueContainer.getConfig().getMaxSize() - queueContainer.size();
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        stats.incrementOtherOperations();
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.REMAINING_CAPACITY;
    }
}

package com.hazelcast.queue.impl.operations;


import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.queue.impl.QueueContainer;
import com.hazelcast.queue.impl.QueueDataSerializerHook;

/**
 * Returns the remaining capacity of the queue based on config max-size
 */
public class RemainingCapacityOperation extends QueueOperation {

    public RemainingCapacityOperation() {
    }

    public RemainingCapacityOperation(final String name) {
        super(name);
    }

    @Override
    public void run() {
        final QueueContainer container = getOrCreateContainer();
        response = container.getConfig().getMaxSize() - container.size();
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

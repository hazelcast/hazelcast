package com.hazelcast.datastream.impl.operations;

import com.hazelcast.datastream.impl.DSPartitionListeners;
import com.hazelcast.util.function.Consumer;

import java.util.concurrent.Executor;

public class AddLocalListenerOperation extends DataStreamOperation {

    private transient Executor executor;
    private transient Consumer consumer;
    private transient long offset;

    public AddLocalListenerOperation() {
    }

    public AddLocalListenerOperation(String name, long offset, Consumer consumer, Executor executor) {
        super(name);
        this.offset = offset;
        this.consumer = consumer;
        this.executor = executor;
    }

    @Override
    public void run() throws Exception {
        DSPartitionListeners subscription = service.getOrCreateSubscription(getName(), getPartitionId(), partition);
        subscription.registerLocalListener(consumer,executor,offset);
    }

    @Override
    public int getId() {
        throw new UnsupportedOperationException();
    }
}

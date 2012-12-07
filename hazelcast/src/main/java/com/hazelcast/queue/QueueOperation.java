package com.hazelcast.queue;

import com.hazelcast.spi.impl.AbstractNamedOperation;

/**
 * @ali 12/6/12
 */
public abstract class QueueOperation extends AbstractNamedOperation {

    transient Object response;
    transient QueueContainer container;

    protected QueueOperation() {
    }

    protected QueueOperation(String name) {
        super(name);
    }

    public void beforeRun() {
        QueueService queueService = getService();
        container = queueService.getContainer(name);
    }

    public Object getResponse() {
        return response;
    }

    public String getServiceName() {
        return QueueService.NAME;
    }
}

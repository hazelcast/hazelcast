package com.hazelcast.queue;

import com.hazelcast.spi.KeyBasedOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

/**
 * @ali 12/6/12
 */
public abstract class QueueOperation extends AbstractNamedOperation implements KeyBasedOperation {

    transient Object response;

    protected QueueOperation() {
    }

    protected QueueOperation(String name) {
        super(name);
    }

    public QueueContainer getContainer() {
        QueueService queueService = getService();
        return queueService.getContainer(name);
    }

    public Object getResponse() {
        return response;
    }

    public String getServiceName() {
        return QueueService.NAME;
    }

    public int getKeyHash() {
        return name.hashCode();
    }


}

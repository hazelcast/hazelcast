package com.hazelcast.queue;

import com.hazelcast.spi.impl.AbstractNamedOperation;

/**
 * Created with IntelliJ IDEA.
 * User: ali
 * Date: 11/19/12
 * Time: 11:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class QueueSizeOperation extends AbstractNamedOperation {

    private transient int size;

    public QueueSizeOperation(){

    }

    public QueueSizeOperation(String name){
        super(name);
    }

    public void run() {
        QueueService queueService = getService();
        size = queueService.getQueue(name).size();
    }

    @Override
    public Object getResponse() {
        return size;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }
}

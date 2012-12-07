package com.hazelcast.queue;

import com.hazelcast.spi.impl.AbstractNamedOperation;

/**
 * User: ali
 * Date: 11/19/12
 * Time: 11:37 AM
 */
public class QueueSizeOperation extends QueueOperation {

    public QueueSizeOperation(){
    }

    public QueueSizeOperation(String name){
        super(name);
    }

    public void run() {
        QueueService queueService = getService();
        response = queueService.getQueue(name).size();
    }
}

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

    public QueueSizeOperation(){

    }

    public QueueSizeOperation(String name){
        super(name);
    }

    public void run() {
        QueueService queueService = getService();
        int size = queueService.getQueue(name).size();
        getResponseHandler().sendResponse(size);
    }
}

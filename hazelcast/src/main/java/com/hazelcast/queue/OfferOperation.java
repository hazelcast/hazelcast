package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:14 AM
 */
public class OfferOperation extends QueueDataOperation {

    public OfferOperation(){

    }

    public OfferOperation(final String name, final Data data){
        super(name, data);
    }

    public void run() {
        response = container.dataQueue.offer(data);
    }

    public Operation getBackupOperation() {
        return new QueueBackupOperation(new OfferOperation(name, data));
    }
}

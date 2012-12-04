package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.BackupOperation;

/**
 * Created with IntelliJ IDEA.
 * User: ali
 * Date: 11/14/12
 * Time: 12:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class OfferOperation extends QueueDataOperation {

    public OfferOperation(){

    }

    public OfferOperation(final String name, final Data data){
        super(name, data);
    }

    public void run() {
        QueueService queueService = getService();
        boolean ok = queueService.getQueue(name).offer(data);
//        getResponseHandler().sendResponse(ok);
    }

    public BackupOperation getBackupOperation() {
        return null;
    }
}

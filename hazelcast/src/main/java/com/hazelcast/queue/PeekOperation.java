package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ali
 * Date: 11/23/12
 * Time: 3:56 AM
 * To change this template use File | Settings | File Templates.
 */
public class PeekOperation extends AbstractNamedOperation {

    private boolean poll;

    public PeekOperation(){

    }

    public PeekOperation(final String name, final boolean poll){
        super(name);
        this.poll = poll;
    }

    public void run() {
        QueueService queueService = getService();
        Data data = null;
        if(poll){
            data = queueService.getQueue(name).poll();
        }
        else {
            data = queueService.getQueue(name).peek();
        }
        getResponseHandler().sendResponse(data);
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(poll);
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        poll = in.readBoolean();
    }
}

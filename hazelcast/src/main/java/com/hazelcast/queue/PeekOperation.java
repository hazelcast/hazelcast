package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: ali
 * Date: 11/23/12
 * Time: 3:56 AM
 */
public class PeekOperation extends QueueOperation {

    public PeekOperation(){
    }

    public PeekOperation(final String name){
        super(name);
    }

    public void run() {
        response = getContainer().dataQueue.peek();
    }
}

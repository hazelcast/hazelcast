package com.hazelcast.impl;

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;

public class MergeClusters extends AbstractRemotelyProcessable {

    private Address newTargetAddress = null;

    public MergeClusters() {
    }

    public MergeClusters(Address newTargetAddress) {
        super();
        this.newTargetAddress = newTargetAddress;
    }

    public void process() {
        if (conn == null) {
            return;
        }

        Address endpoint = conn.getEndPoint();
        if (endpoint == null || !endpoint.equals(getNode().getMasterAddress())) {
            return;
        }

        final ILogger logger = node.loggingService.getLogger(this.getClass().getName());
        logger.log(Level.WARNING, node.address + " is merging [tcp/ip] to " + newTargetAddress + ", because : instructed by master");

        // XXX: Do the restarting work in another thread because doing it on the processable thread doesn't seem to work.
        new Thread(new Runnable() {

            public void run() {
                ThreadContext.get().setCurrentFactory(node.factory);
                node.getJoiner().setTargetAddress(newTargetAddress);
                node.factory.restart();
            }
        }).start();
    }

    public void readData(DataInput in) throws IOException {
        super.readData(in);
        newTargetAddress = new Address();
        newTargetAddress.readData(in);
    }

    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        newTargetAddress.writeData(out);
    }
}

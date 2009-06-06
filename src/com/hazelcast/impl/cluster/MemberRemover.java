/**
 * 
 */
package com.hazelcast.impl.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

public class MemberRemover implements RemotelyProcessable {
    private Address deadAddress = null;

    public MemberRemover() {
    }

    public MemberRemover(Address deadAddress) {
        super();
        this.deadAddress = deadAddress;
    }

    public void process() {
        ClusterManager.get().doRemoveAddress(deadAddress);
    }

    public void setConnection(Connection conn) {
    }

    public void readData(DataInput in) throws IOException {
        deadAddress = new Address();
        deadAddress.readData(in);
    }

    public void writeData(DataOutput out) throws IOException {
        deadAddress.writeData(out);
    }
}
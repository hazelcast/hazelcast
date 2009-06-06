/**
 * 
 */
package com.hazelcast.impl.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.Address;

public class Master extends AbstractRemotelyProcessable {
    public Address address = null;

    public Master() {

    }

    public Master(Address originAddress) {
        super();
        this.address = originAddress;
    }

    @Override
    public void readData(DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        address.writeData(out);
    }

    @Override
    public String toString() {
        return "Master " + address;
    }

    public void process() {
        ClusterManager.get().handleMaster(this);
    }
}
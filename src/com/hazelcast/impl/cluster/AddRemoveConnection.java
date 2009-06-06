/**
 * 
 */
package com.hazelcast.impl.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.Address;

public class AddRemoveConnection extends AbstractRemotelyProcessable {
    public Address address = null;

    public boolean add = true;

    public AddRemoveConnection() {

    }

    public AddRemoveConnection(Address address, boolean add) {
        super();
        this.address = address;
        this.add = add;
    }

    @Override
    public void readData(DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        add = in.readBoolean();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        address.writeData(out);
        out.writeBoolean(add);
    }

    @Override
    public String toString() {
        return "AddRemoveConnection add=" + add + ", " + address;
    }

    public void process() {
        ClusterManager.get().handleAddRemoveConnection(this);
    }
}
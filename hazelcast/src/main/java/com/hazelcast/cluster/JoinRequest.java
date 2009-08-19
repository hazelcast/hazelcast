/**
 *
 */
package com.hazelcast.cluster;

import com.hazelcast.impl.Node;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinRequest extends AbstractRemotelyProcessable {

    protected Node.Type nodeType = Node.Type.MEMBER;
    public Address address;
    public Address to;
    public String groupName;
    public String groupPassword;

    public JoinRequest() {
    }

    public JoinRequest(Address address, String groupName, String groupPassword, Node.Type type) {
        this(null, address, groupName, groupPassword, type);
    }

    public JoinRequest(Address to, Address address, String groupName, String groupPassword, Node.Type type) {
        super();
        this.to = to;
        this.address = address;
        this.groupName = groupName;
        this.groupPassword = groupPassword;
        this.nodeType = type;
    }

    @Override
    public void readData(DataInput in) throws IOException {
        boolean hasTo = in.readBoolean();
        if (hasTo) {
            to = new Address();
            to.readData(in);
        }
        address = new Address();
        address.readData(in);
        nodeType = Node.Type.create(in.readInt());
        groupName = in.readUTF();
        groupPassword = in.readUTF();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        boolean hasTo = (to != null);
        out.writeBoolean(hasTo);
        if (hasTo) {
            to.writeData(out);
        }
        address.writeData(out);
        out.writeInt(nodeType.getValue());
        out.writeUTF(groupName);
        out.writeUTF(groupPassword);
    }

    @Override
    public String toString() {
        return "JoinRequest{" +
                "nodeType=" + nodeType +
                ", address=" + address +
                ", groupName='" + groupName + '\'' +
                ", groupPassword='" + groupPassword + '\'' +
                '}';
    }

    public void process() {
        getNode().clusterManager.handleJoinRequest(this);
    }
}
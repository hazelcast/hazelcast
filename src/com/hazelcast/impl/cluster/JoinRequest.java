/**
 * 
 */
package com.hazelcast.impl.cluster;

import static com.hazelcast.impl.Constants.NodeTypes.NODE_MEMBER;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.Address;

public class JoinRequest extends AbstractRemotelyProcessable {

    protected int nodeType = NODE_MEMBER;
    public Address address;
    public String groupName;
    public String groupPassword;

    public JoinRequest() {
    }

    public JoinRequest(Address address, String groupName, String groupPassword, int type) {
        super();
        this.address = address;
        this.groupName = groupName;
        this.groupPassword = groupPassword;
        this.nodeType = type;
    }

    @Override
    public void readData(DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        nodeType = in.readInt();
        groupName = in.readUTF();
        groupPassword = in.readUTF();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        address.writeData(out);
        out.writeInt(nodeType);
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
        ClusterManager.get().handleJoinRequest(this);
    }
}
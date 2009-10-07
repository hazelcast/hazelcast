/**
 *
 */
package com.hazelcast.cluster;

import com.hazelcast.impl.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MemberInfo implements DataSerializable {
    Address address = null;
    Node.NodeType nodeType = Node.NodeType.MEMBER;

    public MemberInfo() {
    }

    public MemberInfo(Address address) {
        super();
        this.address = address;
    }

    public MemberInfo(Address address, Node.NodeType nodeType) {
        super();
        this.address = address;
        this.nodeType = nodeType;
    }

    public void readData(DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        nodeType = Node.NodeType.create(in.readInt());
    }

    public void writeData(DataOutput out) throws IOException {
        address.writeData(out);
        out.writeInt(nodeType.getValue());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MemberInfo other = (MemberInfo) obj;
        if (address == null) {
            if (other.address != null)
                return false;
        } else if (!address.equals(other.address))
            return false;
        return true;
    }
}
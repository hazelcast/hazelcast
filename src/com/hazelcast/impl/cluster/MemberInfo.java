/**
 * 
 */
package com.hazelcast.impl.cluster;

import static com.hazelcast.impl.Constants.NodeTypes.NODE_MEMBER;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

public class MemberInfo implements DataSerializable {
    Address address = null;
    int nodeType = NODE_MEMBER;

    public MemberInfo() {
    }

    public MemberInfo(Address address, int nodeType) {
        super();
        this.address = address;
        this.nodeType = nodeType;
    }

    public void readData(DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        nodeType = in.readInt();
    }

    public void writeData(DataOutput out) throws IOException {
        address.writeData(out);
        out.writeInt(nodeType);
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
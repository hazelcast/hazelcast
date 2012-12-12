/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cluster;

import com.hazelcast.impl.NodeType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MemberInfo implements DataSerializable {
    Address address = null;
    NodeType nodeType = NodeType.MEMBER;
    String uuid;

    public MemberInfo() {
    }

    public MemberInfo(Address address) {
        super();
        this.address = address;
    }

    public MemberInfo(Address address, NodeType nodeType, String uuid) {
        super();
        this.address = address;
        this.nodeType = nodeType;
        this.uuid = uuid;
    }

    public void readData(DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        nodeType = NodeType.create(in.readInt());
        if (in.readBoolean()) {
            uuid = in.readUTF();
        }
    }

    public void writeData(DataOutput out) throws IOException {
        address.writeData(out);
        out.writeInt(nodeType.getValue());
        boolean hasUuid = uuid != null;
        out.writeBoolean(hasUuid);
        if (hasUuid) {
            out.writeUTF(uuid);
        }
    }

    public Address getAddress() {
        return address;
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

    @Override
    public String toString() {
        return "MemberInfo{" +
                "address=" + address +
                ", nodeType=" + nodeType +
                '}';
    }
}

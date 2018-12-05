/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.core.Member;
import com.hazelcast.cp.CPMember;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.SocketAddress;
import java.net.UnknownHostException;

/**
 * {@code CPMember} represents a member in Raft group.
 * Each member must have a unique address and id in the group.
 */
public class CPMemberInfo implements CPMember, Serializable, IdentifiedDataSerializable {

    private static final long serialVersionUID = 5628148969327743953L;

    private transient String uuid;
    private transient Address address;

    public CPMemberInfo() {
    }

    public CPMemberInfo(String id, Address address) {
        this.uuid = id;
        this.address = address;
    }

    public CPMemberInfo(Member member) {
        this.uuid = member.getUuid();
        this.address = member.getAddress();
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public SocketAddress getSocketAddress() {
        try {
            return address.getInetSocketAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }

    public Address getAddress() {
        return address;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(uuid);
        out.writeUTF(address.getHost());
        out.writeInt(address.getPort());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        uuid = in.readUTF();
        String host = in.readUTF();
        int port = in.readInt();
        address = new Address(host, port);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(uuid);
        out.writeObject(address);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uuid = in.readUTF();
        address = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CP_MEMBER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CPMemberInfo)) {
            return false;
        }

        CPMemberInfo that = (CPMemberInfo) o;

        if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) {
            return false;
        }
        return address != null ? address.equals(that.address) : that.address == null;
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (address != null ? address.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        return "CPMember{" + "uuid=" + uuid + ", address=" + address + '}';
    }
}

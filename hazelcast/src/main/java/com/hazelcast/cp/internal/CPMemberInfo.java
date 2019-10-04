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

import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;

/**
 * {@code CPMember} represents a member in CP group.
 * Each member must have a unique ID and an address.
 */
public class CPMemberInfo implements CPMember, Serializable, IdentifiedDataSerializable {

    private static final long serialVersionUID = 5628148969327743953L;

    private UUID uuid;
    private Address address;
    private transient RaftEndpointImpl endpoint;

    public CPMemberInfo() {
    }

    public CPMemberInfo(UUID uuid, Address address) {
        checkNotNull(uuid);
        checkNotNull(address);
        this.uuid = uuid;
        this.endpoint = new RaftEndpointImpl(uuid);
        this.address = address;
    }

    public CPMemberInfo(Member member) {
        this(member.getUuid(), member.getAddress());
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public Address getAddress() {
        return address;
    }

    public RaftEndpoint toRaftEndpoint() {
        return endpoint;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        writeUUID(out, uuid);
        out.writeUTF(address.getHost());
        out.writeInt(address.getPort());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        uuid = readUUID(in);
        endpoint = new RaftEndpointImpl(uuid);
        String host = in.readUTF();
        int port = in.readInt();
        address = new Address(host, port);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeUUID(out, uuid);
        out.writeObject(address);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uuid = readUUID(in);
        endpoint = new RaftEndpointImpl(uuid);
        address = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.CP_MEMBER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CPMemberInfo that = (CPMemberInfo) o;

        if (!uuid.equals(that.uuid)) {
            return false;
        }
        return address.equals(that.address);
    }

    @Override
    public int hashCode() {
        int result = uuid.hashCode();
        result = 31 * result + address.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CPMember{" + "uuid=" + endpoint.getUuid() + ", address=" + address + '}';
    }
}

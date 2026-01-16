/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.UUID;

import static com.hazelcast.cp.internal.RaftServiceUtil.CP_AUTO_STEP_DOWN_WHEN_LEADER_ATTRIBUTE;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static java.lang.Boolean.parseBoolean;

/**
 * {@code CPMember} represents a member in CP group.
 * Each member must have a unique ID and an address.
 */
public class CPMemberInfo implements CPMember, Serializable, IdentifiedDataSerializable, Versioned {

    @Serial
    private static final long serialVersionUID = 5628148969327743953L;

    private UUID uuid;
    private Address address;
    private boolean autoStepDownWhenLeader;
    private transient RaftEndpointImpl endpoint;

    public CPMemberInfo() {
    }

    public CPMemberInfo(UUID uuid, Address address, boolean autoStepDownWhenLeader) {
        checkNotNull(uuid);
        checkNotNull(address);
        this.uuid = uuid;
        this.endpoint = new RaftEndpointImpl(uuid);
        this.address = address;
        this.autoStepDownWhenLeader = autoStepDownWhenLeader;
    }

    public CPMemberInfo(Member member) {
        this(member.getUuid(), member.getAddress(), parseBoolean(member.getAttributes()
                .getOrDefault(CP_AUTO_STEP_DOWN_WHEN_LEADER_ATTRIBUTE, "false")));
    }

    public CPMemberInfo(UUID uuid, Address address, boolean isAutoStepDownWhenLeaderExists, boolean autoStepDownWhenLeader) {
        this(uuid, address, autoStepDownWhenLeader);
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public Address getAddress() {
        return address;
    }

    @Override
    public boolean isAutoStepDownWhenLeader() {
        return autoStepDownWhenLeader;
    }

    public RaftEndpoint toRaftEndpoint() {
        return endpoint;
    }

    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
        writeUUID(out, uuid);
        out.writeUTF(address.getHost());
        out.writeInt(address.getPort());
        out.writeBoolean(autoStepDownWhenLeader);
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        uuid = readUUID(in);
        endpoint = new RaftEndpointImpl(uuid);
        String host = in.readUTF();
        int port = in.readInt();
        try {
            address = new Address(host, port);
        } catch (UnknownHostException ex) {
            address = Address.createUnresolvedAddress(host, port);
        }
        try {
            autoStepDownWhenLeader = in.readBoolean();
        } catch (EOFException ignore) {
            autoStepDownWhenLeader = false;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeUUID(out, uuid);
        out.writeObject(address);
        if (out.getVersion().isGreaterOrEqual(Versions.V5_7)) {
            out.writeBoolean(autoStepDownWhenLeader);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uuid = readUUID(in);
        endpoint = new RaftEndpointImpl(uuid);
        address = in.readObject();
        // RU_COMPAT_5_6
        if (in.getVersion().isGreaterOrEqual(Versions.V5_7)) {
            autoStepDownWhenLeader = in.readBoolean();
        } else {
            autoStepDownWhenLeader = false;
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceSerializerConstants.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceSerializerConstants.CP_MEMBER;
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
        return "CPMemberInfo{"
                + "uuid=" + uuid
                + ", address=" + address
                + ", autoStepDownWhenLeader=" + autoStepDownWhenLeader
                + '}';
    }
}

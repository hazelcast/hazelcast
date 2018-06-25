/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;

/**
 * Implementation of {@link RaftGroupId}.
 */
public final class RaftGroupIdImpl implements RaftGroupId, IdentifiedDataSerializable {

    private String name;
    private long commitIndex;

    public RaftGroupIdImpl() {
    }

    public RaftGroupIdImpl(String name, long commitIndex) {
        assert name != null;
        this.name = name;
        this.commitIndex = commitIndex;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(commitIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        commitIndex = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.GROUP_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RaftGroupIdImpl)) {
            return false;
        }

        RaftGroupIdImpl that = (RaftGroupIdImpl) o;
        return commitIndex == that.commitIndex && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "RaftGroupId{" + "name='" + name + '\'' + ", commitIndex=" + commitIndex + '}';
    }

    // ----- CLIENT CONVENIENCE METHODS -----
    public static RaftGroupId readFrom(ClientMessage message) {
        String name = message.getStringUtf8();
        long commitIndex = message.getLong();
        return new RaftGroupIdImpl(name, commitIndex);
    }

    public static void writeTo(RaftGroupId groupId, ClientMessage message) {
        message.set(groupId.name());
        message.set(groupId.commitIndex());
    }

    public static int dataSize(RaftGroupId groupId) {
        return ParameterUtil.calculateDataSize(groupId.name()) + Bits.LONG_SIZE_IN_BYTES;
    }
}

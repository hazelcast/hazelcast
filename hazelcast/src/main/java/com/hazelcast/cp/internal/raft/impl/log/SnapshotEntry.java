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

package com.hazelcast.cp.internal.raft.impl.log;

import com.hazelcast.core.Endpoint;
import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

/**
 * Represents a snapshot in the {@link RaftLog}.
 * <p>
 * Snapshot entry is sent to followers via
 * {@link com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot} RPC.
 */
public class SnapshotEntry extends LogEntry implements IdentifiedDataSerializable {
    private long groupMembersLogIndex;
    private Collection<Endpoint> groupMembers;

    public SnapshotEntry() {
    }

    public SnapshotEntry(int term, long index, Object operation,
                         long groupMembersLogIndex, Collection<Endpoint> groupMembers) {
        super(term, index, operation);
        this.groupMembersLogIndex = groupMembersLogIndex;
        this.groupMembers = groupMembers;
    }

    public long groupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    public Collection<Endpoint> groupMembers() {
        return groupMembers;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(groupMembersLogIndex);
        out.writeInt(groupMembers.size());
        for (Endpoint endpoint : groupMembers) {
            out.writeObject(endpoint);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        groupMembersLogIndex = in.readLong();
        int count = in.readInt();
        groupMembers = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            Endpoint endpoint = in.readObject();
            groupMembers.add(endpoint);
        }
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerHook.SNAPSHOT_ENTRY;
    }

    public String toString(boolean detailed) {
        return "SnapshotEntry{" + "term=" + term() + ", index=" + index()
                + (detailed ? ", operation=" + operation() : "")
                + ", groupMembersLogIndex=" + groupMembersLogIndex + ", groupMembers=" + groupMembers + '}';
    }

    @Override
    public String toString() {
        return toString(false);
    }
}

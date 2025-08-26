/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerConstants;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;

import static java.util.Collections.unmodifiableCollection;

/**
 * Represents a snapshot in the {@code RaftLog}.
 * <p>
 * The snapshot entry is sent to followers chunk by
 * chunk using the {@link InstallSnapshotRequest} RPC.
 */
public class SnapshotEntry extends LogEntry implements IdentifiedDataSerializable {

    private long groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;

    public SnapshotEntry() {
    }

    public SnapshotEntry(int term, long index, Object operation,
                         long groupMembersLogIndex,
                         Collection<RaftEndpoint> groupMembers) {
        super(term, index, operation);
        this.groupMembersLogIndex = groupMembersLogIndex;
        this.groupMembers = unmodifiableCollection(new LinkedHashSet<>(groupMembers));
    }

    public long groupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    public Collection<RaftEndpoint> groupMembers() {
        return groupMembers;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(groupMembersLogIndex);
        SerializationUtil.writeCollection(groupMembers, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        groupMembersLogIndex = in.readLong();
        int count = in.readInt();
        groupMembers = new LinkedHashSet<>(count);
        for (int i = 0; i < count; i++) {
            RaftEndpoint endpoint = in.readObject();
            groupMembers.add(endpoint);
        }
        groupMembers = unmodifiableCollection(groupMembers);
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerConstants.SNAPSHOT_ENTRY;
    }

    public String toString(boolean detailed) {
        return "SnapshotEntry{" + "term=" + term() + ", index=" + index()
                + (detailed ? ", operation=" + operation() : "")
                + ", groupMembersLogIndex=" + groupMembersLogIndex
                + ", groupMembers=" + groupMembers + '}';
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public static boolean isNonInitial(SnapshotEntry entry) {
        return entry != null && entry.index() > 0;
    }
}

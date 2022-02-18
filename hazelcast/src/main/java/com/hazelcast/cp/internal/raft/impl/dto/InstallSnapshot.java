/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl.dto;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Struct for InstallSnapshot RPC.
 * <p>
 * See <i>7 Log compaction</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * Invoked by leader to send chunks of a snapshot to a follower.
 * Leaders always send chunks in order.
 */
public class InstallSnapshot implements IdentifiedDataSerializable {

    private RaftEndpoint leader;
    private int term;
    private SnapshotEntry snapshot;
    private long queryRound;

    public InstallSnapshot() {
    }

    public InstallSnapshot(RaftEndpoint leader, int term, SnapshotEntry snapshot, long queryRound) {
        this.leader = leader;
        this.term = term;
        this.snapshot = snapshot;
        this.queryRound = queryRound;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int term() {
        return term;
    }

    public SnapshotEntry snapshot() {
        return snapshot;
    }

    public long queryRound() {
        return queryRound;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerHook.INSTALL_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(leader);
        out.writeInt(term);
        out.writeObject(snapshot);
        out.writeLong(queryRound);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        leader = in.readObject();
        term = in.readInt();
        snapshot = in.readObject();
        queryRound = in.readLong();
    }

    @Override
    public String toString() {
        return "InstallSnapshot{" + "leader=" + leader + ", term=" + term + ", snapshot=" + snapshot + ", queryRound="
                + queryRound + '}';
    }

}

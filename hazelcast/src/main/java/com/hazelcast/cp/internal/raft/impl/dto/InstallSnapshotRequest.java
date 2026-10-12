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

package com.hazelcast.cp.internal.raft.impl.dto;

import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerConstants;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
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
public class InstallSnapshotRequest implements IdentifiedDataSerializable {

    private RaftEndpoint leader;
    private int term;
    private SnapshotChunk snapshotChunk;
    private long queryRound;
    private long flowControlSequenceNumber;

    public InstallSnapshotRequest() {
    }

    public InstallSnapshotRequest(RaftEndpoint leader, int term, SnapshotChunk snapshotChunk,
                                  long queryRound, long flowControlSequenceNumber) {
        this.leader = leader;
        this.term = term;
        this.snapshotChunk = snapshotChunk;
        this.queryRound = queryRound;
        this.flowControlSequenceNumber = flowControlSequenceNumber;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int term() {
        return term;
    }

    public SnapshotChunk snapshotChunk() {
        return snapshotChunk;
    }

    public long queryRound() {
        return queryRound;
    }

    public long flowControlSequenceNumber() {
        return flowControlSequenceNumber;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerConstants.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerConstants.INSTALL_SNAPSHOT_REQUEST;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(leader);
        out.writeInt(term);
        out.writeLong(queryRound);
        out.writeLong(flowControlSequenceNumber);
        out.writeObject(snapshotChunk);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        leader = in.readObject();
        term = in.readInt();
        queryRound = in.readLong();
        flowControlSequenceNumber = in.readLong();
        snapshotChunk = in.readObject();
    }

    @Override
    public String toString() {
        return "InstallSnapshotRequest{"
                + "leader=" + leader
                + ", term=" + term
                + ", snapshotChunk=" + snapshotChunk
                + ", queryRound=" + queryRound
                + ", flowControlSequenceNumber=" + flowControlSequenceNumber
                + '}';
    }
}

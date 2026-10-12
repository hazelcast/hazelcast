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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Response for {@link InstallSnapshotRequest}.
 * <p>
 * See <i>7 Log compaction</i> section of <i>In Search of an Understandable
 * Consensus Algorithm</i> paper by <i>Diego Ongaro</i> and <i>John
 * Ousterhout</i>.
 * <p>
 * A follower can request the missing snapshot chunks in any order from the
 * leader.
 *
 * @see InstallSnapshotRequest
 */
public class InstallSnapshotResponse implements IdentifiedDataSerializable {

    private RaftEndpoint follower;
    private int term;
    private long queryRound;
    private long flowControlSequenceNumber;
    private long snapshotIndex;
    private int requestedChunkNumber;

    public InstallSnapshotResponse() {
    }

    public InstallSnapshotResponse(RaftEndpoint follower, int term, long queryRound,
                                   long flowControlSequenceNumber,
                                   long snapshotIndex, int requestedChunkNumber) {
        this.follower = follower;
        this.term = term;
        this.queryRound = queryRound;
        this.flowControlSequenceNumber = flowControlSequenceNumber;
        this.snapshotIndex = snapshotIndex;
        this.requestedChunkNumber = requestedChunkNumber;
    }

    public RaftEndpoint follower() {
        return follower;
    }

    public int term() {
        return term;
    }

    public long queryRound() {
        return queryRound;
    }

    public long flowControlSequenceNumber() {
        return flowControlSequenceNumber;
    }

    public long snapshotIndex() {
        return snapshotIndex;
    }

    public int requestedChunkNumber() {
        return requestedChunkNumber;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerConstants.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerConstants.INSTALL_SNAPSHOT_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(follower);
        out.writeInt(term);
        out.writeLong(queryRound);
        out.writeLong(flowControlSequenceNumber);
        out.writeLong(snapshotIndex);
        out.writeInt(requestedChunkNumber);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        follower = in.readObject();
        term = in.readInt();
        queryRound = in.readLong();
        flowControlSequenceNumber = in.readLong();
        snapshotIndex = in.readLong();
        requestedChunkNumber = in.readInt();
    }

    @Override
    public String toString() {
        return "InstallSnapshotResponse{"
                + "follower=" + follower
                + ", term=" + term
                + ", queryRound=" + queryRound
                + ", flowControlSequenceNumber=" + flowControlSequenceNumber
                + ", snapshotIndex=" + snapshotIndex
                + ", chunkNumber=" + requestedChunkNumber + "(0-based)"
                + "}";
    }
}

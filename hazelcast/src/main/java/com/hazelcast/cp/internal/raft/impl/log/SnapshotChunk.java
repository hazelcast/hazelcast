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

package com.hazelcast.cp.internal.raft.impl.log;

import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerConstants;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents a chunk of a snapshot.
 * <p>
 * A snapshot is divided into multiple chunks
 * for efficient transmission and storage.
 * <p>
 * This class includes metadata about the chunk, such as its
 * position in the sequence of chunks, the total number of
 * chunks, and the group members associated with the snapshot.
 */
public class SnapshotChunk extends LogEntry {

    /**
     * The position of this chunk in the sequence of chunks.
     * <p>
     * Chunk numbering starts from 1.
     */
    private int chunkNumber;

    /**
     * The total number of chunks in the snapshot.
     */
    private int chunkCount;

    /**
     * The log index at which the group members were recorded.
     */
    private long groupMembersLogIndex;

    /**
     * The collection of Raft endpoints representing the group members at the time of the snapshot.
     */
    private Collection<RaftEndpoint> groupMembers;

    /**
     * Default constructor for deserialization purposes.
     */
    public SnapshotChunk() {
    }

    /**
     * Returns the total number of chunks in the snapshot.
     *
     * @return the total number of chunks
     */
    public int chunkCount() {
        return chunkCount;
    }

    /**
     * Returns the position of this chunk in the sequence of chunks.
     *
     * @return the chunk number, starting from 1
     */
    public int chunkNumber() {
        return chunkNumber;
    }

    /**
     * Checks if this chunk is the first chunk in the sequence.
     *
     * @return {@code true} if this is the first chunk, {@code false} otherwise
     */
    public boolean isFirstChunk() {
        return chunkNumber() == 0;
    }

    /**
     * Returns the collection of Raft endpoints representing the group members at the time of the snapshot.
     *
     * @return the group members
     */
    public Collection<RaftEndpoint> groupMembers() {
        return groupMembers;
    }

    /**
     * Returns the log index at which the group members were recorded.
     *
     * @return the log index of the group members
     */
    public long groupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    /**
     * Sets the group members associated with this snapshot chunk.
     *
     * @param groupMembers the group members to set
     * @return this instance for method chaining
     */
    public SnapshotChunk setGroupMembers(Collection<RaftEndpoint> groupMembers) {
        this.groupMembers = groupMembers;
        return this;
    }

    /**
     * Sets the log index at which the group members were recorded.
     *
     * @param groupMembersLogIndex the log index to set
     * @return this instance for method chaining
     */
    public SnapshotChunk setGroupMembersLogIndex(long groupMembersLogIndex) {
        this.groupMembersLogIndex = groupMembersLogIndex;
        return this;
    }

    /**
     * Sets the position of this chunk in the sequence of chunks.
     *
     * @param chunkNumber the chunk number to set, starting from 0
     * @return this instance for method chaining
     */
    public SnapshotChunk setChunkNumber(int chunkNumber) {
        assert chunkNumber >= 0 && chunkNumber < chunkCount
                : "not expected chunkNumber=" + chunkNumber + "(0-based), chunkCount=" + chunkCount;

        this.chunkNumber = chunkNumber;
        return this;
    }

    /**
     * Sets the total number of chunks in the snapshot.
     *
     * @param chunkCount the total number of chunks to set
     * @return this instance for method chaining
     */
    public SnapshotChunk setChunkCount(int chunkCount) {
        this.chunkCount = chunkCount;
        return this;
    }

    /**
     * Serializes this snapshot chunk to the provided {@link ObjectDataOutput}.
     *
     * @param out the output stream to write to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(chunkNumber);
        out.writeInt(chunkCount);
        out.writeLong(groupMembersLogIndex);
        SerializationUtil.writeCollection(groupMembers, out);
    }

    /**
     * Deserializes this snapshot chunk from the provided {@link ObjectDataInput}.
     *
     * @param in the input stream to read from
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        chunkNumber = in.readInt();
        chunkCount = in.readInt();
        groupMembersLogIndex = in.readLong();
        groupMembers = SerializationUtil.readCollection(in);
    }

    /**
     * Returns the class ID for serialization purposes.
     *
     * @return the class ID
     */
    @Override
    public int getClassId() {
        return RaftDataSerializerConstants.SNAPSHOT_CHUNK;
    }

    /**
     * Returns a string representation of this snapshot chunk.
     *
     * @return a string representation of this object
     */
    @Override
    public String toString() {
        return "SnapshotChunk{"
                + "term=" + term()
                + ", snapshotIndex=" + index()
                + ", operation=" + (operation() == null ? "null" : "not null")
                + ", chunkNumber=" + chunkNumber + "(0-based)"
                + ", chunkCount=" + chunkCount
                + ", groupMembersLogIndex=" + groupMembersLogIndex
                + ", groupMembers=" + groupMembers
                + "}";
    }
}

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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotRequest;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.Comparator.comparingInt;

/**
 * Mutable state of one chunked snapshot transfer <b>RECEIVED</b> by a Raft node.
 * It owns the transfer identity, source leader, snapshot metadata, received
 * chunks, and the ordered set of chunk numbers that are still missing.
 *
 * <p>Not thread-safe.
 *
 * @see ChunkedSnapshotInstaller
 */
final class SnapshotTransferState {
    private static final long NO_SNAPSHOT_INDEX = -1;

    /**
     * Identifies the leader term and snapshot represented by this transfer.
     *
     * @param leaderTerm    Raft term in which the sender acts as leader
     * @param snapshotTerm  term of the last log entry included in the snapshot
     * @param snapshotIndex index of the last log entry included in the snapshot
     */
    private record SnapshotTransferId(int leaderTerm,
                                      int snapshotTerm,
                                      long snapshotIndex) {
    }

    private final SnapshotTransferId id;
    private final RaftEndpoint leader;
    private final int totalChunks;
    private final long groupMembersLogIndex;
    private final Collection<RaftEndpoint> groupMembers;
    private final List<SnapshotChunk> receivedChunks = new ArrayList<>();
    private final Set<Integer> missingChunks = new LinkedHashSet<>();
    private final long startNanos;

    private SnapshotTransferState(SnapshotTransferId id, RaftEndpoint leader, int totalChunks,
                                  long groupMembersLogIndex, Collection<RaftEndpoint> groupMembers) {
        this.id = id;
        this.leader = leader;
        this.totalChunks = totalChunks;
        this.groupMembersLogIndex = groupMembersLogIndex;
        this.groupMembers = groupMembers;
        this.startNanos = id == null ? 0 : System.nanoTime();
        IntStream.range(0, totalChunks).forEach(missingChunks::add);
    }

    /**
     * Creates an empty state representing the absence of an active transfer.
     */
    static SnapshotTransferState empty() {
        return new SnapshotTransferState(null, null, 0, 0, null);
    }

    /**
     * Creates transfer state from the identity and snapshot metadata carried by
     * the given request.
     */
    static SnapshotTransferState from(InstallSnapshotRequest request) {
        SnapshotChunk chunk = request.snapshotChunk();
        SnapshotTransferId id = new SnapshotTransferId(
                request.term(), chunk.term(), chunk.index());
        return new SnapshotTransferState(
                id, request.leader(), chunk.chunkCount(), chunk.groupMembersLogIndex(), chunk.groupMembers());
    }

    /**
     * Returns whether the active transfer was started in the given leader term.
     * Raft election safety permits at most one elected leader per term, so the
     * term is sufficient to identify an ownership change. The leader endpoint
     * is retained separately only for routing transfer responses.
     */
    boolean hasLeaderTerm(int leaderTerm) {
        return id != null && id.leaderTerm() == leaderTerm;
    }

    RaftEndpoint leader() {
        return leader;
    }

    /**
     * {@link System#nanoTime()} when this transfer attempt started.
     */
    long startNanos() {
        return startNanos;
    }

    long snapshotIndex() {
        return id == null ? NO_SNAPSHOT_INDEX : id.snapshotIndex();
    }

    int snapshotTerm() {
        assert id != null : "snapshot term is available only for an active transfer";
        return id.snapshotTerm();
    }

    int totalChunks() {
        return totalChunks;
    }

    long groupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    Collection<RaftEndpoint> groupMembers() {
        return groupMembers;
    }

    List<SnapshotChunk> receivedChunks() {
        return receivedChunks;
    }

    Set<Integer> missingChunks() {
        return missingChunks;
    }

    boolean hasMissingChunks() {
        return !missingChunks.isEmpty();
    }

    boolean acceptChunkNumber(int chunkNumber) {
        return missingChunks.remove(chunkNumber);
    }

    void markChunkMissing(int chunkNumber) {
        missingChunks.add(chunkNumber);
    }

    void trackReceivedChunk(SnapshotChunk chunk) {
        receivedChunks.add(chunk);
    }

    int receivedChunkCount() {
        return receivedChunks.size();
    }

    int missingChunkCount() {
        return missingChunks.size();
    }

    void sortReceivedChunks() {
        receivedChunks.sort(comparingInt(SnapshotChunk::chunkNumber));
    }
}

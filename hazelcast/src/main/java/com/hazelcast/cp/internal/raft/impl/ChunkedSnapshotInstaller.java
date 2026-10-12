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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotRequest;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotResponse;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.internal.util.IterableUtil;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.util.List;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Manages the installation of snapshot chunks received from the Raft leader.
 * <p>
 * One instance is created per {@link RaftNodeImpl} to handle the complete lifecycle
 * of snapshot installation:
 * <ol>
 *   <li>Receiving and validating chunks</li>
 *   <li>Tracking missing chunks</li>
 *   <li>Assembling complete snapshots</li>
 *   <li>Installing snapshots into the Raft log</li>
 * </ol>
 *
 * <p>The manager ensures snapshots are installed atomically and maintains consistency
 * even when chunks arrive out of order or some chunks are missing initially.
 *
 * @see RaftNodeImpl
 * @see SnapshotChunk
 * @since 5.6
 */
public class ChunkedSnapshotInstaller {
    private static final int UNSET = -1;

    private final RaftNodeImpl raftNode;
    private final RaftStateStore stateStore;
    private final ILogger logger;

    private SnapshotTransferState transferState = SnapshotTransferState.empty();
    private long lastSnapshotTransferDurationMs;
    private long snapshotTransferCount;

    public ChunkedSnapshotInstaller(RaftNodeImpl raftNode) {
        this.raftNode = raftNode;
        this.logger = raftNode.getLogger(getClass());
        this.stateStore = raftNode.state().stateStore();
    }

    /**
     * Processes a snapshot chunk from the leader.
     *
     * @param request The snapshot chunk installation request
     */
    public void processChunk(InstallSnapshotRequest request) {
        if (ChunkProcessingDecision.ABORT == validateAndStoreChunk(request)) {
            return;
        }

        if (hasMissingChunks()) {
            requestMissingChunks(request);
            return;
        }

        try {
            installCompleteSnapshot();
            notifySuccess(request);
            recordTransferCompletion();
            logger.info(format("Successfully installed snapshot [index=%d, term=%d] from leader %s",
                    transferState.snapshotIndex(), transferState.snapshotTerm(), request.leader()));
        } catch (Throwable t) {
            logger.severe(format("Failed to install snapshot [index=%d, term=%d]: %s",
                    transferState.snapshotIndex(), transferState.snapshotTerm(), t.getMessage()));
            throw t;
        } finally {
            reset();
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private ChunkProcessingDecision validateAndStoreChunk(InstallSnapshotRequest request) {
        SnapshotChunk chunk = request.snapshotChunk();

        if (isStaleChunk(chunk)) {
            logger.fine("Ignoring stale chunk - received index %d is behind our current commit index %d",
                    chunk.index(), raftNode.state().commitIndex());
            return ChunkProcessingDecision.ABORT;
        }

        if (isDuplicateCommitIndex(chunk)) {
            notifySuccess(request);
            logger.fine("Skipping chunk - index %d matches our current commit index %d (already applied)",
                    chunk.index(), raftNode.state().commitIndex());
            return ChunkProcessingDecision.ABORT;
        }

        if (isLeaderTermChanged(request)) {
            logger.info(format("Starting new snapshot installation from leader %s in request term %d "
                            + "- switching from index %d to index %d",
                    request.leader(), request.term(), transferState.snapshotIndex(), chunk.index()));
            startNewSnapshotTransfer(request);
        }

        if (isFromOlderSnapshot(chunk)) {
            logger.fine("Rejecting old snapshot chunk - received snapshot [term=%d, index=%d, "
                            + "chunk=%d(0-based)/%d], is older than current snapshot index: %d",
                    chunk.term(), chunk.index(), chunk.chunkNumber(), chunk.chunkCount(), transferState.snapshotIndex());
            return ChunkProcessingDecision.ABORT;
        }

        if (isFromNewerSnapshot(chunk)) {
            logger.info(String.format("Starting new snapshot installation - switching from index %d to newer index %d",
                    transferState.snapshotIndex(), chunk.index()));
            startNewSnapshotTransfer(request);
        }

        if (isMetadataChunk(chunk)) {
            logger.fine("Skip metadata chunk [index: %d, chunk: %d (0-based)] "
                            + "(triggers snapshot fetch, may resend on retry). ",
                    chunk.index(), chunk.chunkNumber());
            return ChunkProcessingDecision.CONTINUE;
        }

        if (!isTermConsistent(chunk)) {
            throw new IllegalStateException(format(
                    "Term mismatch detected! Snapshot index %d expects term %d but received term %d. "
                            + "This indicates a serious consistency issue.",
                    chunk.index(), transferState.snapshotTerm(), chunk.term()));
        }

        if (!transferState.acceptChunkNumber(chunk.chunkNumber())) {
            logger.fine("Received duplicate chunk %d (0-based) for snapshot index %d term %d "
                            + "(current progress: %d of %d chunks received, %d still missing)",
                    chunk.chunkNumber(), chunk.index(), chunk.term(), transferState.receivedChunkCount(),
                    transferState.totalChunks(), transferState.missingChunkCount());
            return ChunkProcessingDecision.CONTINUE;
        }

        persistAndTrackChunk(chunk);
        logger.info(format("Successfully stored chunk %d (0-based) of %d for snapshot index %d term %d "
                        + "(%d of %d chunks complete, %d remaining)",
                chunk.chunkNumber(), transferState.totalChunks(), transferState.snapshotIndex(), transferState.snapshotTerm(),
                transferState.receivedChunkCount(), transferState.totalChunks(), transferState.missingChunkCount()));
        return ChunkProcessingDecision.CONTINUE;
    }

    private void installCompleteSnapshot() {
        SnapshotEntry snapshot = flushAndBuildSnapshotEntry();
        raftNode.getRaftIntegration().resetForChunkedSnapshotRestore();
        raftNode.installSnapshot(snapshot);
    }

    private void requestMissingChunks(InstallSnapshotRequest request) {
        assert hasMissingChunks() : "always checked before method call";

        final int nextMissingChunkNumber = IterableUtil.getFirst(transferState.missingChunks(), UNSET);
        assert nextMissingChunkNumber != UNSET : "never expect an unset missingChunkNumber";

        RaftState state = raftNode.state();

        InstallSnapshotResponse response = new InstallSnapshotResponse(
                state.localEndpoint(),
                state.term(),
                request.queryRound(),
                request.flowControlSequenceNumber(),
                transferState.snapshotIndex(),
                nextMissingChunkNumber
        );

        raftNode.getRaftIntegration()
                .send(response, transferState.leader());
    }

    private boolean isStaleChunk(SnapshotChunk chunk) {
        return chunk.index() < raftNode.state().commitIndex();
    }

    private boolean isDuplicateCommitIndex(SnapshotChunk chunk) {
        return chunk.index() == raftNode.state().commitIndex();
    }

    private boolean isFromOlderSnapshot(SnapshotChunk chunk) {
        return chunk.index() < transferState.snapshotIndex();
    }

    private boolean isFromNewerSnapshot(SnapshotChunk chunk) {
        return chunk.index() > transferState.snapshotIndex();
    }

    private boolean isMetadataChunk(SnapshotChunk chunk) {
        return chunk.operation() == null;
    }

    private boolean isTermConsistent(SnapshotChunk chunk) {
        return chunk.term() == transferState.snapshotTerm();
    }

    private boolean isLeaderTermChanged(InstallSnapshotRequest request) {
        return !transferState.hasLeaderTerm(request.term());
    }

    private void startNewSnapshotTransfer(InstallSnapshotRequest request) {
        cleanupPreviousSnapshot();
        transferState = SnapshotTransferState.from(request);
    }

    private void persistAndTrackChunk(SnapshotChunk chunk) {
        assert chunk.operation() != null : "Can never be null " + chunk;

        try {
            stateStore.persistSnapshotChunk(chunk);
            transferState.trackReceivedChunk(chunk);
        } catch (IOException e) {
            transferState.markChunkMissing(chunk.chunkNumber());
            logger.severe(format("Failed to persist snapshot chunk [index=%d, term=%d, chunk=%d(0-based)]: %s",
                    chunk.index(), chunk.term(), chunk.chunkNumber(), e.getMessage()), e);
            throw new HazelcastException(e);
        }
    }

    private void recordTransferCompletion() {
        lastSnapshotTransferDurationMs = NANOSECONDS.toMillis(System.nanoTime() - transferState.startNanos());
        snapshotTransferCount++;
    }

    /**
     * Wall-clock duration (ms) of the most recently completed snapshot transfer installed from a leader.
     * Holds its last value between transfers.
     */
    public long getLastSnapshotTransferDurationMs() {
        return lastSnapshotTransferDurationMs;
    }

    /**
     * Cumulative number of snapshot transfers successfully installed from a leader.
     */
    public long getSnapshotTransferCount() {
        return snapshotTransferCount;
    }

    private void reset() {
        transferState = SnapshotTransferState.empty();
    }

    private void cleanupPreviousSnapshot() {
        if (!CollectionUtil.isEmpty(transferState.receivedChunks())) {
            try {
                stateStore.deleteSnapshotChunks(transferState.snapshotIndex());
            } catch (IOException e) {
                logger.warning(String.format("Failed to delete previous snapshot chunks for index %d: %s",
                        transferState.snapshotIndex(), e.getMessage()), e);
                throw new HazelcastException(e);
            }
        }
    }

    private SnapshotEntry flushAndBuildSnapshotEntry() {
        transferState.sortReceivedChunks();
        try {
            stateStore.flushLogs();
        } catch (IOException e) {
            throw new HazelcastException(e);
        }

        return new SnapshotEntry(transferState.snapshotTerm(), transferState.snapshotIndex(),
                transferState.receivedChunks(), transferState.groupMembersLogIndex(), transferState.groupMembers());
    }

    private void notifySuccess(InstallSnapshotRequest request) {
        AppendSuccessResponse response = new AppendSuccessResponse(
                raftNode.getLocalMember(),
                request.term(),
                request.snapshotChunk().index(),
                request.queryRound(),
                request.flowControlSequenceNumber()
        );
        raftNode.send(response, request.leader());
    }

    private boolean hasMissingChunks() {
        return transferState.hasMissingChunks();
    }

    /**
     * Decision on whether to proceed with or abort chunk processing.
     */
    private enum ChunkProcessingDecision {
        /**
         * Continue processing this chunk and the overall snapshot installation
         */
        CONTINUE,

        /**
         * Abort processing this chunk (stale, duplicate, or already handled)
         */
        ABORT
    }

    List<SnapshotChunk> getReceivedChunks() {
        return transferState.receivedChunks();
    }

}

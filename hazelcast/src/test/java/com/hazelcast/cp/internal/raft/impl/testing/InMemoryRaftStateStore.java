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

package com.hazelcast.cp.internal.raft.impl.testing;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;

import static com.hazelcast.cp.internal.raft.impl.log.RaftLog.newRaftLog;

public class InMemoryRaftStateStore implements RaftStateStore {

    private RaftEndpoint localEndpoint;
    private Collection<RaftEndpoint> initialMembers;
    private int term;
    private RaftEndpoint votedFor;
    private SnapshotPersistenceState snapshotPersistenceState;

    private final RaftLog raftLog;

    public InMemoryRaftStateStore(int capacity) {
        this.raftLog = newRaftLog(capacity);
    }

    @Override
    public void open() {
    }

    @Override
    public synchronized void persistInitialMembers(
            @Nonnull RaftEndpoint localMember,
            @Nonnull Collection<RaftEndpoint> initialMembers
    ) {
        this.localEndpoint = localMember;
        this.initialMembers = initialMembers;
    }

    @Override
    public synchronized void persistTerm(int term, @Nonnull RaftEndpoint votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override
    public synchronized void persistEntry(@Nonnull LogEntry entry) {
        raftLog.appendEntries(entry);
    }

    @Override
    public synchronized void persistSnapshotChunk(Object snapshotChunk) {
        SnapshotChunk chunk = (SnapshotChunk) snapshotChunk;

        if (snapshotPersistenceState == null || snapshotPersistenceState.snapshotIndex != chunk.index()) {
            snapshotPersistenceState = new SnapshotPersistenceState(chunk.term(), chunk.index(),
                    chunk.chunkCount(), chunk.groupMembersLogIndex(), chunk.groupMembers());
        }

        snapshotPersistenceState.chunks.put(chunk.chunkNumber(), chunk);
    }

    @Override
    public synchronized void deleteSnapshotChunks(long snapshotIndex) {
        if (snapshotPersistenceState != null && snapshotPersistenceState.snapshotIndex == snapshotIndex) {
            snapshotPersistenceState = null;
        }
    }

    @Override
    public synchronized void deleteEntriesFrom(long startIndexInclusive) {
        raftLog.deleteEntriesFrom(startIndexInclusive);
    }

    @Override
    public synchronized void flushLogs() {
        if (snapshotPersistenceState != null) {
            SnapshotEntry entry = snapshotPersistenceState.toSnapshotEntry();
            if (entry != null) {
                raftLog.setSnapshot(entry);
            }
            snapshotPersistenceState = null;
        }
    }

    @Override
    public void close() {
    }

    public synchronized RestoredRaftState toRestoredRaftState() {
        LogEntry[] entries;
        if (raftLog.snapshotIndex() < raftLog.lastLogOrSnapshotIndex()) {
            entries = raftLog.getEntriesBetween(raftLog.snapshotIndex() + 1, raftLog.lastLogOrSnapshotIndex());
        } else {
            entries = new LogEntry[0];
        }

        return new RestoredRaftState(localEndpoint, initialMembers, term, votedFor, raftLog.snapshot(), entries);
    }

    private static class SnapshotPersistenceState {

        final int term;
        final long snapshotIndex;
        final int chunkCount;
        private long groupMembersLogIndex;
        private Collection<RaftEndpoint> groupMembers;
        final NavigableMap<Integer, SnapshotChunk> chunks = new TreeMap<>();

        SnapshotPersistenceState(int term, long snapshotIndex, int chunkCount,
                                 long groupMembersLogIndex, Collection<RaftEndpoint> groupMembers) {
            this.term = term;
            this.snapshotIndex = snapshotIndex;
            this.chunkCount = chunkCount;
            this.groupMembers = groupMembers;
            this.groupMembersLogIndex = groupMembersLogIndex;
        }

        boolean isCompleted() {
            return chunks.size() == chunkCount;
        }

        SnapshotEntry toSnapshotEntry() {
            if (isCompleted()) {
                ArrayList<SnapshotChunk> snapshotChunks = new ArrayList<>(chunks.values());
                return new SnapshotEntry(term, snapshotIndex,
                        snapshotChunks, groupMembersLogIndex, groupMembers);
            }

            return null;
        }

    }

}

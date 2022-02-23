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

package com.hazelcast.cp.internal.raft.impl.testing;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.hazelcast.cp.internal.raft.impl.log.RaftLog.newRaftLog;

public class InMemoryRaftStateStore implements RaftStateStore {

    private RaftEndpoint localEndpoint;
    private Collection<RaftEndpoint> initialMembers;
    private int term;
    private RaftEndpoint votedFor;
    private RaftLog raftLog;

    public InMemoryRaftStateStore(int capacity) {
        this.raftLog = newRaftLog(capacity);
    }

    @Override
    public void open() {
    }

    @Override
    public void persistInitialMembers(
            @Nonnull RaftEndpoint localMember,
            @Nonnull Collection<RaftEndpoint> initialMembers
    ) {
        this.localEndpoint = localMember;
        this.initialMembers = initialMembers;
    }

    @Override
    public void persistTerm(int term, @Nonnull RaftEndpoint votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override
    public void persistEntry(@Nonnull LogEntry entry) {
        raftLog.appendEntries(entry);
    }

    @Override
    public void persistSnapshot(@Nonnull SnapshotEntry entry) {
        raftLog.setSnapshot(entry);
    }

    @Override
    public void deleteEntriesFrom(long startIndexInclusive) {
        raftLog.deleteEntriesFrom(startIndexInclusive);
    }

    @Override
    public void flushLogs() {
    }

    @Override
    public void close() {
    }

    public RestoredRaftState toRestoredRaftState() {
        LogEntry[] entries;
        if (raftLog.snapshotIndex() < raftLog.lastLogOrSnapshotIndex()) {
            entries = raftLog.getEntriesBetween(raftLog.snapshotIndex() + 1, raftLog.lastLogOrSnapshotIndex());
        } else {
            entries = new LogEntry[0];
        }

        return new RestoredRaftState(localEndpoint, initialMembers, term, votedFor, raftLog.snapshot(), entries);
    }

}

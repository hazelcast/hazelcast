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

package com.hazelcast.cp.internal.raft.impl.testing;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftLogStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;

import java.util.Collection;

import static com.hazelcast.cp.internal.raft.impl.log.RaftLog.newRaftLog;

public class InMemoryRaftStateStore implements RaftStateStore {

    private RaftEndpoint localEndpoint;
    private Collection<RaftEndpoint> initialMembers;
    private int term;
    private RaftEndpoint votedFor;
    private int voteTerm;
    private InMemoryRaftLogStore raftLogStore;

    public InMemoryRaftStateStore(int capacity) {
        raftLogStore = new InMemoryRaftLogStore(capacity);
    }

    @Override
    public void writeTermAndVote(int currentTerm, RaftEndpoint votedFor, int voteTerm) {
        this.term = currentTerm;
        this.votedFor = votedFor;
        this.voteTerm = voteTerm;
    }

    @Override
    public void writeInitialMembers(RaftEndpoint localMember, Collection<RaftEndpoint> initialMembers) {
        this.localEndpoint = localMember;
        this.initialMembers = initialMembers;
    }

    @Override
    public RaftLogStore getRaftLogStore() {
        return raftLogStore;
    }

    @Override
    public void close() {
    }

    public RestoredRaftState toRestoredRaftState() {
        RaftLog raftLog = raftLogStore.raftLog;
        LogEntry[] entries;
        if (raftLog.snapshotIndex() < raftLog.lastLogOrSnapshotIndex()) {
            entries = raftLog.getEntriesBetween(raftLog.snapshotIndex() + 1, raftLog.lastLogOrSnapshotIndex());
        } else {
            entries = new LogEntry[0];
        }

        return new RestoredRaftState(localEndpoint, initialMembers, term, votedFor, voteTerm, raftLog.snapshot(), entries);
    }

    public class InMemoryRaftLogStore implements RaftLogStore {

        private RaftLog raftLog;

        InMemoryRaftLogStore(int capacity) {
            this.raftLog = newRaftLog(capacity);
        }

        @Override
        public void appendEntry(LogEntry entry) {
            raftLog.appendEntries(entry);
        }

        @Override
        public void flush() {
        }

        @Override
        public void writeSnapshot(SnapshotEntry entry) {
            raftLog.setSnapshot(entry);
        }

        @Override
        public void truncateEntriesFrom(long indexInclusive) {
            raftLog.truncateEntriesFrom(indexInclusive);
        }

        @Override
        public void close() {
        }
    }
}

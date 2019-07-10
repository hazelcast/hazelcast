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

package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;

import java.io.IOException;
import java.util.Collection;

/**
 * Used when a Raft node works transiently (its state is not persisted).
 */
public final class NopRaftStateStore implements RaftStateStore {

    /**
     * Non-persisting {@link RaftStateStore} instance
     */
    public static final RaftStateStore INSTANCE = new NopRaftStateStore();

    /**
     * Non-persisting {@link RaftLogStore} instance
     */
    public static final RaftLogStore NOP_LOG_STORE = new RaftLogStore() {
        @Override
        public void open() {
        }

        @Override
        public void appendEntry(LogEntry entry) {
        }

        @Override
        public void flush() {
        }

        @Override
        public void writeSnapshot(SnapshotEntry entry) {
        }

        @Override
        public void truncateEntriesFrom(long indexInclusive) {
        }

        @Override
        public void close() {
        }
    };

    private NopRaftStateStore() {
    }

    @Override
    public void writeTermAndVote(int currentTerm, RaftEndpoint votedFor) {
    }

    @Override
    public void writeInitialMembers(RaftEndpoint localMember, Collection<RaftEndpoint> initialMembers) {
    }

    @Override
    public RaftLogStore getRaftLogStore() {
        return NOP_LOG_STORE;
    }

    @Override
    public void close() throws IOException {
    }
}

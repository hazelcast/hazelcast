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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public interface RaftStateStore extends Closeable {

    /**
     * Initializes the store before starting to persist Raft state.
     * This method is called before any other method in this interface.
     * The state store must be ready to persist Raft state once this method
     * returns.
     *
     * @throws IOException
     */
    void open() throws IOException;

    /**
     * Persists the given local Raft endpoint and initial Raft group members.
     * This method returns after the given arguments are written to disk.
     *
     * @param localMember
     * @param initialMembers
     * @throws IOException
     */
    void persistInitialMembers(RaftEndpoint localMember, Collection<RaftEndpoint> initialMembers) throws IOException;

    /**
     * Persists the term and the Raft endpoint that is voted in the given term.
     * This method returns after the given arguments are written to disk.
     *
     * @param term
     * @param votedFor
     * @throws IOException
     */
    void persistTerm(int term, RaftEndpoint votedFor) throws IOException;

    /**
     *
     * @param entry
     * @throws IOException
     */
    void persistEntry(LogEntry entry) throws IOException;

    // uncommittedEntryCountToRejectNewAppends
    /**
     *
     * @param entry
     * @throws IOException
     */
    void persistSnapshot(SnapshotEntry entry) throws IOException;

    // uncommittedEntryCountToRejectNewAppends
    /**
     *
     * @param indexInclusive
     * @throws IOException
     */
    void truncateEntriesFrom(long indexInclusive) throws IOException;

    /**
     * Forces all buffered (in any layer) Raft log changes to be written
     * to the storage layer. Returns after those changes are written.
     */
    void flushLogs() throws IOException;

}

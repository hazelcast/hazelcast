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

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/**
 * Defines the contract that must be implemented by the
 * storage implementation.
 */
public interface RaftStateStore extends Closeable {

    /**
     * Initializes the store before starting to persist Raft state.
     * This method is called before any other method in this interface.
     * The state store must be ready to persist Raft state once this method
     * returns.
     */
    void open() throws IOException;

    /**
     * Persists the given local Raft endpoint and initial Raft group members.
     * This method returns after the given arguments are written to disk.
     */
    void persistInitialMembers(RaftEndpoint localMember, Collection<RaftEndpoint> initialMembers) throws IOException;

    /**
     * Persists the term and the Raft endpoint that is voted in the given term.
     * This method returns after the given arguments are written to disk.
     */
    void persistTerm(int term, RaftEndpoint votedFor) throws IOException;

    /**
     * Persists the given log entry.
     * <p>
     * Log entries are appended to the Raft log with sequential log indices.
     * The first log index is 1.
     * <p>
     * There will be no gaps in persisted log indices before the very first
     * {@link #persistSnapshot(SnapshotEntry)} call and after each
     * {@link #persistSnapshot(SnapshotEntry)}.
     * <p>
     * Persisted entries can be truncated from tail via
     * {@link #truncateEntriesFrom(long)} in some rare failure scenarios.
     * Consider the following case where 4 log entries are persisted and then
     * a truncation call is made:
     * - persistEntry(1)
     * - persistEntry(2)
     * - persistEntry(3)
     * - persistEntry(4)
     * - truncatedEntriesFrom(3)
     * After this call sequence, log indices will remain sequential
     * and the next persistEntry() call will be for index=3.
     *
     * @see #flushLogs()
     * @see #persistSnapshot(SnapshotEntry)
     * @see #truncateEntriesFrom(long)
     * @see RaftAlgorithmConfig
     */
    void persistEntry(LogEntry entry) throws IOException;

    /**
     * Persists the given snapshot entry.
     * <p>
     * After a snapshot is persisted at index=i and {@link #flushLogs()} is
     * called, the log entries before index=i (inclusive) are not needed
     * anymore and can be truncated from the storage. If truncation is not
     * done, it will not cause a consistency problem but it will increase
     * the recovery duration. Therefore, truncation can be done in a background
     * task.
     * <p>
     * Snapshots are taken at certain indices, which are determined via
     * {@link RaftAlgorithmConfig#getCommitIndexAdvanceCountToSnapshot()}.
     * For instance, if it is 100, snapshots are taken at indices 100, 200,
     * 300, 400 and so on.
     * <p>
     * The snapshot index correlates to the highest persisted log index
     * in 2 different ways:
     * - It can be smaller than or equal to the highest persisted log index
     * if a Raft node takes a snapshot locally. The difference between
     * the snapshot index and the highest persisted log index cannot be greater
     * than {@link RaftAlgorithmConfig#getCommitIndexAdvanceCountToSnapshot()}.
     * For instance, if
     * {@link RaftAlgorithmConfig#getCommitIndexAdvanceCountToSnapshot()} is 50
     * and {@link RaftAlgorithmConfig#getUncommittedEntryCountToRejectNewAppends()}
     * is 10, and a persistSnapshot() call is made for index=100, the highest
     * index given to a previous persistEntry() call can be 110.
     * - The snapshot index can be greater than the highest log entry log index
     * if a follower Raft node installs a snapshot sent by the leader.
     * For the same example above, if the highest persisted log index is 25,
     * the next call can be a snapshot with index=100.
     *
     * @see #flushLogs()
     * @see #persistEntry(LogEntry)
     * @see RaftAlgorithmConfig
     */
    void persistSnapshot(SnapshotEntry entry) throws IOException;

    /**
     * Truncates persisted log entries after the given start index (inclusive).
     * A truncated log entry is no longer valid and must not be restored
     * (or must be certainly ignored during the restore process).
     * <p>
     * The truncation index can be smaller than or equal to the index of
     * the highest persisted log entry index. There is a bound on the number
     * of entries that can be truncated, which is specified by
     * {@link RaftAlgorithmConfig#getUncommittedEntryCountToRejectNewAppends()}.
     * Say that it is 5 and the highest persisted log entry index is 20. Then,
     * at most 5 entries can be truncated from the tail, hence truncation can
     * start from index=16 (but cannot precede it).
     *
     * @see #flushLogs()
     * @see #persistEntry(LogEntry)
     * @see RaftAlgorithmConfig
     */
    void truncateEntriesFrom(long startIndexInclusive) throws IOException;

    /**
     * Forces all buffered (in any layer) Raft log changes to be written
     * to the storage layer and returns after those changes are written.
     *
     * @see #persistEntry(LogEntry)
     * @see #persistSnapshot(SnapshotEntry)
     * @see #truncateEntriesFrom(long)
     */
    void flushLogs() throws IOException;

}

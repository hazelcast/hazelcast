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

package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/**
 * Defines the contract of the Raft storage.
 */
public interface RaftStateStore extends Closeable {

    /**
     * Initializes the store before starting to persist Raft state. This method
     * is called before any other method in this interface. After this method
     * returns, the state store must be ready to accept all other method calls.
     */
    void open() throws IOException;

    /**
     * Persists the given local Raft endpoint and initial Raft group members.
     * When this method returns, all the provided data has become durable.
     */
    void persistInitialMembers(
            @Nonnull RaftEndpoint localMember, @Nonnull Collection<RaftEndpoint> initialMembers
    ) throws IOException;

    /**
     * Persists the term and the Raft endpoint that the local node voted for in
     * the given term. When this method returns, all the provided data has
     * become durable.
     */
    void persistTerm(int term, @Nullable RaftEndpoint votedFor) throws IOException;

    /**
     * Persists the given log entry.
     * <p>
     * Log entries are appended to the Raft log with sequential log indices.
     * The first log index is 1.
     * <p>
     * A block of consecutive log entries has no gaps in the indices, but a
     * gap can appear between the snapshot entry and its preceding regular
     * entry. This happens in an edge case where a follower has fallen so far
     * behind that the missing entries are no longer available from the leader.
     * In that case the leader will send its snapshot entry instead.
     * <p>
     * In a rare failure scenario Raft must delete a range of the newest
     * entries, rolling back the index of the next persisted entry. Consider
     * the following case where Raft persists three log entries and then
     * deletes entries from index 2:
     * <ul>
     *     <li> persistEntry(1)
     *     <li> persistEntry(2)
     *     <li> persistEntry(3)
     *     <li> deleteEntriesFrom(2)
     * </ul>
     * After this call sequence log indices will remain sequential
     * and the next persistEntry() call will be for index=2.
     *
     * @see #flushLogs()
     * @see #persistSnapshot(SnapshotEntry)
     * @see #deleteEntriesFrom(long)
     * @see RaftAlgorithmConfig
     */
    void persistEntry(@Nonnull LogEntry entry) throws IOException;

    /**
     * Persists the given snapshot entry.
     * <p>
     * After a snapshot is persisted at <em>index=i</em> and {@link #flushLogs()}
     * is called, the log entry at <em>index=i</em> and all the preceding
     * entries are no longer needed and can be evicted from storage. Failing to
     * evict stale entries will not cause a consistency problem, but it will
     * increase the time to recover after a restart. Therefore eviction can be
     * done in a background task.
     * <p>
     * Raft takes snapshots at a predetermined interval, controlled by {@link
     * RaftAlgorithmConfig#getCommitIndexAdvanceCountToSnapshot()
     * commitIndexAdvanceCountToSnapshot}. For instance, if it is 100, snapshots
     * will occur at indices 100, 200, 300, and so on.
     * <p>
     * The snapshot index can lag behind the index of the newest log entry that
     * was already persisted, but there is an upper bound to this difference,
     * controlled by {@link RaftAlgorithmConfig#getUncommittedEntryCountToRejectNewAppends()
     * uncommittedEntryCountToRejectNewAppends}. For instance, if {@code
     * uncommittedEntryCountToRejectNewAppends} is 10, and a {@code
     * persistSnapshot()} call is made with snapshotIndex=100, the index of the
     * preceding {@code persistEntry()} call can be at most 110.
     * <p>
     * On the other hand, the snapshot index can also be ahead of the newest log
     * entry. This can happen when a Raft follower has fallen so far behind the
     * leader that the leader no longer holds the missing entries. In that case
     * the follower receives a snapshot from the leader. There is no upper bound
     * on the gap between the newest log entry and the index of the received
     * snapshot.
     * </ul>
     *
     * @see #flushLogs()
     * @see #persistEntry(LogEntry)
     * @see RaftAlgorithmConfig
     */
    void persistSnapshot(@Nonnull SnapshotEntry entry) throws IOException;

    /**
     * Rolls back the log by deleting all entries starting with the given index.
     * A deleted log entry is no longer valid and must not be restored (or at
     * least must be ignored during the restore process).
     * <p>
     * There is a bound on the number of entries that can be deleted, specified
     * by {@link RaftAlgorithmConfig#getUncommittedEntryCountToRejectNewAppends()
     * uncommittedEntryCountToRejectNewAppends}. Say that it is 5 and the
     * highest persisted log entry index is 20. At most 5 newest entries can be
     * deleted, hence deletion can start at index=16 or higher.
     *
     * @see #flushLogs()
     * @see #persistEntry(LogEntry)
     * @see RaftAlgorithmConfig
     */
    void deleteEntriesFrom(long startIndexInclusive) throws IOException;

    /**
     * Forces all buffered (in any layer) Raft log changes to be written
     * to the storage layer and returns after those changes are written.
     *
     * @see #persistEntry(LogEntry)
     * @see #persistSnapshot(SnapshotEntry)
     * @see #deleteEntriesFrom(long)
     */
    void flushLogs() throws IOException;

}

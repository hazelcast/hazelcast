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

package com.hazelcast.cp.internal.raft.impl.log;

import com.hazelcast.ringbuffer.impl.ArrayRingbuffer;
import com.hazelcast.ringbuffer.impl.Ringbuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code RaftLog} keeps and maintains Raft log entries and snapshot. Entries
 * appended in leader's RaftLog are replicated to all followers in the same
 * order and all nodes in a Raft group eventually keep the exact same
 * copy of the RaftLog.
 * <p>
 * Raft maintains the following properties, which together constitute
 * the LogMatching Property:
 * <ul>
 * <li>If two entries in different logs have the same index and term, then
 * they store the same command.</li>
 * <li>If two entries in different logs have the same index and term, then
 * the logs are identical in all preceding entries.</li>
 * </ul>
 *
 * @see LogEntry
 * @see SnapshotEntry
 */
public class RaftLog {

    /**
     * Array of log entries stored in the Raft log.
     * <p/>
     * Important: Log entry indices start from 1, not 0.
     */
    private final Ringbuffer<LogEntry> logs;

    /**
     * Latest snapshot entry. Initially snapshot is empty.
     */
    private SnapshotEntry snapshot = new SnapshotEntry();

    public RaftLog(int capacity) {
        logs = new ArrayRingbuffer<LogEntry>(capacity);
    }

    /**
     * Returns the last entry index in the Raft log,
     * either from the last log entry or from the last snapshot
     * if no logs are available.
     */
    public long lastLogOrSnapshotIndex() {
        return lastLogOrSnapshotEntry().index();
    }

    /**
     * Returns the last term in the Raft log,
     * either from the last log entry or from the last snapshot
     * if no logs are available.
     */
    public int lastLogOrSnapshotTerm() {
        return lastLogOrSnapshotEntry().term();
    }

    /**
     * Returns the last entry in the Raft log,
     * either from the last log entry or from the last snapshot
     * if no logs are available.
     */
    public LogEntry lastLogOrSnapshotEntry() {
        return !logs.isEmpty() ? logs.read(logs.tailSequence()) : snapshot;
    }

    /**
     * Returns true if the log contains an entry at {@code entryIndex},
     * false otherwise.
     * <p>
     * Important: Log entry indices start from 1, not 0.
     */
    public boolean containsLogEntry(long entryIndex) {
        long sequence = toSequence(entryIndex);
        return sequence >= logs.headSequence() && sequence <= logs.tailSequence();
    }

    /**
     * Returns the log entry stored at {@code entryIndex}. Entry is retrieved
     * only from the current log, not from the snapshot entry.
     * <p>
     * If no entry available at this index, then {@code null} is returned.
     * <p>
     * Important: Log entry indices start from 1, not 0.
     */
    public LogEntry getLogEntry(long entryIndex) {
        if (entryIndex < 1) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ". Index starts from 1.");
        }
        if (!containsLogEntry(entryIndex)) {
            return null;
        }

        LogEntry logEntry = logs.read(toSequence(entryIndex));
        assert logEntry.index() == entryIndex : "Expected: " + entryIndex + ", Entry: " + logEntry;
        return logEntry;
    }

    /**
     * Truncates log entries with indexes {@code >= entryIndex}.
     *
     * @return truncated log entries
     * @throws IllegalArgumentException If no entries are available to
     *                                  truncate, if {@code entryIndex} is
     *                                  greater than last log index or smaller
     *                                  than snapshot index.
     */
    public List<LogEntry> truncateEntriesFrom(long entryIndex) {
        if (entryIndex <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", snapshot index: " + snapshotIndex());
        }
        if (entryIndex > lastLogOrSnapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", last log index: " + lastLogOrSnapshotIndex());
        }

        long startSequence = toSequence(entryIndex);
        assert startSequence >= logs.headSequence() : "Entry index: " + entryIndex + ", Head Seq: " + logs.headSequence();

        List<LogEntry> truncated = new ArrayList<LogEntry>();
        for (long ix = startSequence; ix <= logs.tailSequence(); ix++) {
            truncated.add(logs.read(ix));
        }
        logs.setTailSequence(startSequence - 1);

        return truncated;
    }

    /**
     * Returns the number of empty indices in the Raft log
     */
    public int availableCapacity() {
        return (int) (logs.getCapacity() - logs.size());
    }

    /**
     * Returns true if the Raft log contains empty indices for the requested amount
     */
    public boolean checkAvailableCapacity(int requestedCapacity) {
        return availableCapacity() >= requestedCapacity;
    }

    /**
     * Appends new entries to the Raft log.
     *
     * @throws IllegalArgumentException If an entry is appended with a lower
     *                                  term than the last term in the log or
     *                                  if an entry is appended with an index
     *                                  not equal to
     *                                  {@code index == lastIndex + 1}.
     */
    public void appendEntries(LogEntry... newEntries) {
        int lastTerm = lastLogOrSnapshotTerm();
        long lastIndex = lastLogOrSnapshotIndex();

        if (!checkAvailableCapacity(newEntries.length)) {
            throw new IllegalStateException("Not enough capacity! Capacity: " + logs.getCapacity()
                    + ", Size: " + logs.size() + ", New entries: " + newEntries.length);
        }

        for (LogEntry entry : newEntries) {
            if (entry.term() < lastTerm) {
                throw new IllegalArgumentException("Cannot append " + entry + " since its term is lower than last log term: "
                        + lastTerm);
            }
            if (entry.index() != lastIndex + 1) {
                throw new IllegalArgumentException("Cannot append " + entry
                        + " since its index is bigger than (lastLogIndex + 1): " + (lastIndex + 1));
            }
            logs.add(entry);
            lastIndex++;
            lastTerm = Math.max(lastTerm, entry.term());
        }
    }

    /**
     * Returns log entries between {@code fromEntryIndex} and {@code toEntryIndex}, both inclusive.
     *
     * @throws IllegalArgumentException If {@code fromEntryIndex} is greater
     *                                  than {@code toEntryIndex}, or
     *                                  if {@code fromEntryIndex} is equal to /
     *                                  smaller than {@code snapshotIndex},
     *                                  or if {@code fromEntryIndex} is greater
     *                                  than last log index,
     *                                  or if {@code toEntryIndex} is greater
     *                                  than last log index.
     */
    public LogEntry[] getEntriesBetween(long fromEntryIndex, long toEntryIndex) {
        if (fromEntryIndex > toEntryIndex) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex + ", to entry index: "
                    + toEntryIndex);
        }
        if (!containsLogEntry(fromEntryIndex)) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex);
        }
        if (fromEntryIndex > lastLogOrSnapshotIndex()) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex + ", last log index: "
                    + lastLogOrSnapshotIndex());
        }
        if (toEntryIndex > lastLogOrSnapshotIndex()) {
            throw new IllegalArgumentException("Illegal to entry index: " + toEntryIndex + ", last log index: "
                    + lastLogOrSnapshotIndex());
        }

        assert ((int) (toEntryIndex - fromEntryIndex)) >= 0 : "Int overflow! From: " + fromEntryIndex + ", to: " + toEntryIndex;
        LogEntry[] entries = new LogEntry[(int) (toEntryIndex - fromEntryIndex + 1)];
        long offset = toSequence(fromEntryIndex);

        for (int i = 0; i < entries.length; i++) {
            entries[i] = logs.read(offset + i);
        }
        return entries;
    }

    /**
     * Installs the snapshot entry and truncates log entries those are included
     * in snapshot (entries whose indexes are smaller than
     * the snapshot's index).
     *
     * @return truncated log entries after snapshot is installed
     * @throws IllegalArgumentException if the snapshot's index is smaller than
     *                                  or equal to current snapshot index
     */
    public int setSnapshot(SnapshotEntry snapshot) {
        return setSnapshot(snapshot, snapshot.index());
    }

    public int setSnapshot(SnapshotEntry snapshot, long truncateUpToIndex) {
        if (snapshot.index() <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + snapshot.index() + ", current snapshot index: "
                    + snapshotIndex());
        }

        long newHeadSeq = toSequence(truncateUpToIndex) + 1;
        long newTailSeq = Math.max(logs.tailSequence(), newHeadSeq - 1);

        long prevSize = logs.size();
        // Set truncated slots to null to reduce memory usage.
        // Otherwise this has no effect on correctness.
        for (long seq = logs.headSequence(); seq < newHeadSeq; seq++) {
            logs.set(seq, null);
        }
        logs.setHeadSequence(newHeadSeq);
        logs.setTailSequence(newTailSeq);

        this.snapshot = snapshot;
        return (int) (prevSize - logs.size());
    }

    /**
     * Returns snapshot entry index.
     */
    public long snapshotIndex() {
        return snapshot.index();
    }

    /**
     * Returns snapshot entry.
     */
    public SnapshotEntry snapshot() {
        return snapshot;
    }

    private long toSequence(long entryIndex) {
        return entryIndex - 1;
    }
}

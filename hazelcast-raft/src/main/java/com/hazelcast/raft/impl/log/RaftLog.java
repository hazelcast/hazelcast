/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.log;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.reverse;

/**
 * {@code RaftLog} keeps and maintains Raft log entries and snapshot. Entries appended in leader's RaftLog
 * are replicated to all followers in the same order and all nodes in a Raft group eventually keep the exact same
 * copy of the RaftLog.
 * <p>
 * Raft maintains the following properties, which together constitute the LogMatching Property:
 * <ul>
 * <li>If two entries in different logs have the same index and term, then they store the same command.</li>
 * <li>If two entries in different logs have the same index and term, then the logs are identical in all preceding entries.</li>
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
    private final ArrayList<LogEntry> logs = new ArrayList<LogEntry>();

    private SnapshotEntry snapshot = new SnapshotEntry();

    /**
     * Returns the last entry index in the Raft log,
     * either from the last log entry or from the last snapshot if no logs are available.
     */
    public long lastLogOrSnapshotIndex() {
        return lastLogOrSnapshotEntry().index();
    }

    /**
     * Returns the last term in the Raft log,
     * either from the last log entry or from the last snapshot if no logs are available.
     */
    public int lastLogOrSnapshotTerm() {
        return lastLogOrSnapshotEntry().term();
    }

    /**
     * Returns the last entry in the Raft log,
     * either from the last log entry or from the last snapshot if no logs are available.
     */
    public LogEntry lastLogOrSnapshotEntry() {
        return logs.size() > 0 ? logs.get(logs.size() - 1) : snapshot;
    }

    /**
     * Returns the log entry stored at {@code entryIndex}. Entry is retrieved only from the current log,
     * not from the snapshot entry.
     * <p>
     * If no entry available at this index, then {@code null} is returned.
     * <p>
     * Important: Log entry indices start from 1, not 0.
     */
    public LogEntry getLogEntry(long entryIndex) {
        if (entryIndex < 1) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ". Index starts from 1.");
        }
        if (entryIndex > lastLogOrSnapshotIndex() || snapshotIndex() >= entryIndex) {
            return null;
        }

        return logs.get(toArrayIndex(entryIndex));
    }

    /**
     * Truncates log entries with indexes {@code >= entryIndex}.
     *
     * @return truncated log entries
     * @throws IllegalArgumentException If no entries are available to truncate, if {@code entryIndex} is greater
     *                                  than last log index or smaller than snapshot index.
     */
    public List<LogEntry> truncateEntriesFrom(long entryIndex) {
        if (entryIndex <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", snapshot index: " + snapshotIndex());
        }
        if (entryIndex > lastLogOrSnapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", last log index: " + lastLogOrSnapshotIndex());
        }

        List<LogEntry> truncated = new ArrayList<LogEntry>();
        for (int i = logs.size() - 1, j = toArrayIndex(entryIndex); i >= j; i--) {
            truncated.add(logs.remove(i));
        }

        reverse(truncated);

        return truncated;
    }

    /**
     * Appends new entries to the Raft log.
     *
     * @throws IllegalArgumentException If an entry is appended with a lower term than the last term in the log
     *                                  or if an entry is appended with an index not equal to {@code index == lastIndex + 1}.
     */
    public void appendEntries(LogEntry... newEntries) {
        int lastTerm = lastLogOrSnapshotTerm();
        long lastIndex = lastLogOrSnapshotIndex();

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
     * @throws IllegalArgumentException If {@code fromEntryIndex} is greater than {@code toEntryIndex}
     *                                  or if {@code fromEntryIndex} is equal to / smaller than {@code snapshotIndex}
     *                                  or if {@code fromEntryIndex} is greater than last log index
     *                                  or if {@code toEntryIndex} is greater than last log index.
     */
    public LogEntry[] getEntriesBetween(long fromEntryIndex, long toEntryIndex) {
        if (fromEntryIndex > toEntryIndex) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex + ", to entry index: "
                    + toEntryIndex);
        }
        if (fromEntryIndex <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex + ", snapshot index: "
                    + snapshotIndex());
        }
        if (fromEntryIndex > lastLogOrSnapshotIndex()) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex + ", last log index: "
                    + lastLogOrSnapshotIndex());
        }
        if (toEntryIndex > lastLogOrSnapshotIndex()) {
            throw new IllegalArgumentException("Illegal to entry index: " + toEntryIndex + ", last log index: "
                    + lastLogOrSnapshotIndex());
        }

        return logs.subList(toArrayIndex(fromEntryIndex), toArrayIndex(toEntryIndex + 1)).toArray(new LogEntry[0]);
    }

    /**
     * Installs the snapshot entry and truncates log entries those are included in snapshot
     * (entries whose indexes are smaller than the snapshot's index).
     *
     * @return truncated log entries after snapshot is installed
     * @throws IllegalArgumentException if the snapshot's index is smaller than or equal to current snapshot index
     */
    public List<LogEntry> setSnapshot(SnapshotEntry snapshot) {
        if (snapshot.index() <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + snapshot.index() + ", current snapshot index: "
                    + snapshotIndex());
        }

        List<LogEntry> truncated = new ArrayList<LogEntry>();
        reverse(logs);
        for (int i = logs.size() - 1; i >= 0; i--) {
            LogEntry logEntry = logs.get(i);
            if (logEntry.index() > snapshot.index()) {
                break;
            }

            logs.remove(i);
        }

        reverse(logs);

        this.snapshot = snapshot;

        return truncated;
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

    private int toArrayIndex(long entryIndex) {
        return (int) (entryIndex - snapshotIndex()) - 1;
    }
}

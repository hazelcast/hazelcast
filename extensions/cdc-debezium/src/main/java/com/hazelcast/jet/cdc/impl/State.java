/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.cdc.impl;

import io.debezium.relational.history.HistoryRecord;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;

public final class State {
    private static final Map<Long, State> STATES = new ConcurrentHashMap<>();

    /**
     * Key represents the partition which the record originated from. <br>
     * Value represents the offset within that partition.
     */
    private final Map<ByteBuffer, ByteBuffer> partitionsToOffset;

    /**
     * We use a copy-on-write-list because it will be written on a
     * different thread (some internal Debezium snapshot thread) than
     * is normally used to run the connector (one of Jet's blocking
     * worker threads).
     * <p>
     * The performance penalty of copying the list is also acceptable
     * since this list will be written rarely after the initial snapshot,
     * only on table schema changes.
     */
    private final List<HistoryRecord> historyRecords;

    /**
     * ID of job for which this state is hold.
     */
    private long jobId;

    State() {
        this(new ConcurrentHashMap<>(), new CopyOnWriteArrayList<>());
    }

    public State(long jobId) {
        this();
        this.jobId = jobId;
    }

    State(Map<ByteBuffer, ByteBuffer> partitionsToOffset, CopyOnWriteArrayList<HistoryRecord> historyRecords) {
        this.partitionsToOffset = partitionsToOffset;
        this.historyRecords = historyRecords;
    }

    @Nonnull
    static State getOrCreate(final long jobId) {
        return requireNonNull(STATES.computeIfAbsent(jobId, k -> new State(jobId)), "state returned cannot be null");
    }

    @Nonnull
    static State get(final long jobId) {
        return requireNonNull(STATES.get(jobId), "state returned cannot be null");
    }

    ByteBuffer getOffset(ByteBuffer partition) {
        return partitionsToOffset.get(partition);
    }

    void setOffset(ByteBuffer partition, ByteBuffer offset) {
        partitionsToOffset.put(partition, offset);
    }

    Map<ByteBuffer, ByteBuffer> getPartitionsToOffset() {
        return partitionsToOffset;
    }

    List<HistoryRecord> getHistoryRecords() {
        return historyRecords;
    }

    void restore(State value) {
        partitionsToOffset.putAll(value.partitionsToOffset);
        historyRecords.addAll(value.historyRecords);
    }

    void addHistory(HistoryRecord record) {
        historyRecords.add(record);
    }

    void remove() {
        STATES.remove(jobId);
    }

    @Override
    public String toString() {
        return "State {"
                + "\n\tjobId=" + jobId
                + "\n\tpartitionsToOffset=" + Utils.decode(partitionsToOffset)
                + ", \n\thistoryRecords=" + historyRecords
                + "\n}";
    }

}

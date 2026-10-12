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
    private static final Map<String, State> STATES = new ConcurrentHashMap<>();

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
     * Unique ID of the source processor owning this state.
     */
    private String sourceId;

    State() {
        this(new ConcurrentHashMap<>(), new CopyOnWriteArrayList<>());
    }

    public State(String sourceId) {
        this();
        this.sourceId = sourceId;
    }

    State(Map<ByteBuffer, ByteBuffer> partitionsToOffset, CopyOnWriteArrayList<HistoryRecord> historyRecords) {
        this.partitionsToOffset = partitionsToOffset;
        this.historyRecords = historyRecords;
    }

    @Nonnull
    static State getOrCreate(final String sourceId) {
        return requireNonNull(STATES.computeIfAbsent(sourceId, State::new), "state returned cannot be null");
    }

    @Nonnull
    static State get(final String sourceId) {
        return requireNonNull(STATES.get(sourceId), "state returned cannot be null");
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
        STATES.remove(sourceId, this);
    }

    @Override
    public String toString() {
        return "State {"
                + "\n\tsourceId=" + sourceId
                + "\n\tpartitionsToOffset=" + Utils.decode(partitionsToOffset)
                + ", \n\thistoryRecords=" + historyRecords
                + "\n}";
    }

}

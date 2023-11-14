/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.connect.impl;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

class State implements Serializable {

    /**
     * Key represents the partition which the record originated from. Value
     * represents the offset within that partition. Kafka Connect represents
     * the partition and offset as arbitrary values so that is why it is
     * stored as map.
     * See {@link SourceRecord} for more information regarding the format.
     */
    private final Map<Map<String, ?>, Map<String, ?>> partitionsToOffset;
    private final Map<Map<String, ?>, Long> partitionsToLastOffsetTime;

    State() {
        this.partitionsToOffset = new ConcurrentHashMap<>();
        this.partitionsToLastOffsetTime = new ConcurrentHashMap<>();
    }

    /**
     * just for testing
     */
    State(Map<Map<String, ?>, Map<String, ?>> partitionsToOffset) {
        this.partitionsToOffset = new ConcurrentHashMap<>(partitionsToOffset);
        this.partitionsToLastOffsetTime = new ConcurrentHashMap<>();
    }

    void commitRecord(SourceRecord rec) {
        Map<String, ?> key = rec.sourcePartition();
        partitionsToOffset.put(key, rec.sourceOffset());
        partitionsToLastOffsetTime.put(key, System.currentTimeMillis());
    }

    void load(State state) {
        for (Map<String, ?> partition : partitionsToOffset.keySet()) {
            if (containsNewerStateFor(partition, state)) {
                partitionsToOffset.put(partition, state.partitionsToOffset.get(partition));
                partitionsToLastOffsetTime.put(partition, state.partitionsToLastOffsetTime.get(partition));
            }
        }
        partitionsToOffset.putAll(state.partitionsToOffset);
    }

    boolean containsNewerStateFor(Map<String, ?> partition, State other) {
        Long thisLastTime = partitionsToLastOffsetTime.get(partition);
        Long otherLastTime = other.partitionsToLastOffsetTime.get(partition);
        if (thisLastTime == null) {
            return false;
        }
        return otherLastTime == null || thisLastTime.longValue() == otherLastTime.longValue();
    }

    Map<String, ?> getOffset(Map<String, ?> partition) {
        return partitionsToOffset.get(partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        State state = (State) o;
        return partitionsToOffset.equals(state.partitionsToOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionsToOffset);
    }

    @Override
    public String toString() {
        return "State{" +
                "partitionsToOffset=" + partitionsToOffset + ',' +
                "partitionsToLastOffsetTime=" + partitionsToLastOffsetTime +
                '}';
    }
}

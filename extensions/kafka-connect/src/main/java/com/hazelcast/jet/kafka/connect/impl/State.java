/*
 * Copyright 2024 Hazelcast Inc.
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.joining;

public class State implements Serializable {

    private List<Map<String, String>> taskConfigs;

    /**
     * Key represents the partition which the record originated from. Value
     * represents the offset within that partition. Kafka Connect represents
     * the partition and offset as arbitrary values so that is why it is
     * stored as map.
     * See {@link SourceRecord} for more information regarding the format.
     */
    private final Map<Map<String, ?>, Map<String, ?>> partitionsToOffset;

    State() {
        this.partitionsToOffset = new ConcurrentHashMap<>();
    }

    /**
     * just for testing
     */
    State(Map<Map<String, ?>, Map<String, ?>> partitionsToOffset) {
        this.partitionsToOffset = new ConcurrentHashMap<>(partitionsToOffset);
    }

    public void setTaskConfigs(List<Map<String, String>> taskConfigs) {
        this.taskConfigs = taskConfigs;
    }

    public Map<String, String> getTaskConfig(int order) {
        if (order < taskConfigs.size()) {
            return taskConfigs.get(order);
        } else {
            return null;
        }
    }

    void commitRecord(SourceRecord rec) {
        partitionsToOffset.put(rec.sourcePartition(), rec.sourceOffset());
    }

    void load(State state) {
        partitionsToOffset.putAll(state.partitionsToOffset);
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
        return Objects.equals(taskConfigs, state.taskConfigs) &&
               Objects.equals(partitionsToOffset, state.partitionsToOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskConfigs, partitionsToOffset);
    }

    @Override
    public String toString() {
        return "State{" +
               "taskConfigs names=" + extractNames(taskConfigs) +
               ", partitionsToOffset=" + partitionsToOffset +
               '}';
    }

    private String extractNames(List<Map<String, String>> taskConfigs) {
        if (taskConfigs == null) {
            return "null";
        }
        return taskConfigs.stream()
                .map(c -> c.getOrDefault("name", "<unnamed>"))
                .collect(joining(",", "[", "]"));
    }
}

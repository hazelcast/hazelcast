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

package com.hazelcast.cluster.impl;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Vector clock consisting of distinct replica logical clocks.
 * <p>
 * See https://en.wikipedia.org/wiki/Vector_clock
 * The vector clock may be read from different thread but concurrent
 * updates must be synchronized externally. There is no guarantee for
 * concurrent updates.
 */
public class VectorClock implements IdentifiedDataSerializable {
    private final Map<String, Long> replicaTimestamps = new ConcurrentHashMap<String, Long>();

    public VectorClock() {
    }

    public VectorClock(VectorClock from) {
        replicaTimestamps.putAll(from.replicaTimestamps);
    }

    /**
     * Returns logical timestamp for given {@code replicaId}.
     * This method may be called from different threads and the result reflects
     * the latest update on the vector clock.
     */
    public Long getTimestampForReplica(String replicaId) {
        return replicaTimestamps.get(replicaId);
    }

    /**
     * Sets the logical timestamp for the given {@code replicaId}.
     * This method is not thread safe and concurrent access must be synchronized
     * externally.
     */
    public void setReplicaTimestamp(String replicaId, long timestamp) {
        replicaTimestamps.put(replicaId, timestamp);
    }

    /**
     * Merges the provided vector clock into this one by taking the maximum of
     * the logical timestamps for each replica.
     * This method is not thread safe and concurrent access must be synchronized
     * externally.
     */
    public void merge(VectorClock other) {
        for (Entry<String, Long> entry : other.replicaTimestamps.entrySet()) {
            final String replicaId = entry.getKey();
            final long mergingTimestamp = entry.getValue();
            final long localTimestamp = replicaTimestamps.containsKey(replicaId)
                    ? replicaTimestamps.get(replicaId)
                    : Long.MIN_VALUE;
            replicaTimestamps.put(replicaId, Math.max(localTimestamp, mergingTimestamp));
        }
    }

    /**
     * Returns {@code true} if this vector clock is causally strictly after the
     * provided vector clock. This means that it the provided clock is neither
     * equal to, greater than or concurrent to this vector clock.
     * This method may be called from different threads and the result reflects
     * the latest update on the vector clock.
     */
    public boolean isAfter(VectorClock other) {
        boolean anyTimestampGreater = false;
        for (Entry<String, Long> otherEntry : other.replicaTimestamps.entrySet()) {
            final String replicaId = otherEntry.getKey();
            final Long otherReplicaTimestamp = otherEntry.getValue();
            final Long localReplicaTimestamp = this.getTimestampForReplica(replicaId);

            if (localReplicaTimestamp == null || localReplicaTimestamp < otherReplicaTimestamp) {
                return false;
            } else if (localReplicaTimestamp > otherReplicaTimestamp) {
                anyTimestampGreater = true;
            }
        }
        // there is at least one local timestamp greater or local vector clock has additional timestamps
        return anyTimestampGreater || !other.replicaTimestamps.keySet().containsAll(replicaTimestamps.keySet());
    }

    /**
     * Returns {@code true} if this vector clock is empty (there are no logical
     * timestamps for any replica).
     * This method may be called from different threads and the result reflects
     * the latest update on the vector clock.
     */
    public boolean isEmpty() {
        return this.replicaTimestamps.isEmpty();
    }

    /** Returns a set of replica logical timestamps for this vector clock. */
    public Set<Entry<String, Long>> entrySet() {
        return replicaTimestamps.entrySet();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(replicaTimestamps.size());
        for (Entry<String, Long> timestampEntry : replicaTimestamps.entrySet()) {
            final String replicaId = timestampEntry.getKey();
            final Long timestamp = timestampEntry.getValue();
            out.writeUTF(replicaId);
            out.writeLong(timestamp);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        final int stateSize = in.readInt();
        for (int i = 0; i < stateSize; i++) {
            final String replicaId = in.readUTF();
            final long timestamp = in.readLong();
            replicaTimestamps.put(replicaId, timestamp);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VectorClock that = (VectorClock) o;

        return replicaTimestamps.equals(that.replicaTimestamps);
    }

    @Override
    public int hashCode() {
        return replicaTimestamps.hashCode();
    }

    @Override
    public String toString() {
        return replicaTimestamps.toString();
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.VECTOR_CLOCK;
    }
}

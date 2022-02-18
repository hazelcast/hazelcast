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

package com.hazelcast.internal.crdt;

import com.hazelcast.cluster.impl.VectorClock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Contains the vector clocks for tracking successfully replicated state
 * for all CRDTs and to any replica.
 * <p>
 * The CRDT state updates can either be a mutation invoked by the user or a merge with a
 * CRDT received by a replication operation.
 *
 * @see CRDTReplicationAwareService
 */
class ReplicatedVectorClocks {
    /**
     * Map which contains the information on last successfully replicated CRDT
     * states for all CRDTs and to all replicas. The map can be used to determine
     * which CRDT state has not yet been replicated and to which replicas.
     */
    private ConcurrentMap<ReplicatedVectorClockId, Map<String, VectorClock>> replicatedVectorClocks
            = new ConcurrentHashMap<ReplicatedVectorClockId, Map<String, VectorClock>>();

    /**
     * Returns the vector clocks for the given {@code serviceName} and
     * {@code memberUUID}.
     * The vector clock map contains mappings from CRDT name to the last
     * successfully replicated CRDT vector clock. All CRDTs in this map should
     * be of the same type.
     * If there are no vector clocks for the given parameters, this method
     * returns an empty map.
     *
     * @param serviceName the service name
     * @param memberUUID  the target member UUID
     * @return the last successfully replicated CRDT vector clocks or an empty
     * map if the CRDTs have not yet been replicated to this member
     * @see CRDTReplicationAwareService
     */
    public Map<String, VectorClock> getReplicatedVectorClock(String serviceName, UUID memberUUID) {
        final ReplicatedVectorClockId id = new ReplicatedVectorClockId(serviceName, memberUUID);
        final Map<String, VectorClock> clocks = replicatedVectorClocks.get(id);
        return clocks != null ? clocks : Collections.emptyMap();
    }

    /**
     * Sets the vector clock map for the given {@code serviceName} and
     * {@code memberUUID}.
     * The vector clock map contains mappings from CRDT name to the last
     * successfully replicated CRDT state version. All CRDTs in this map should
     * be of the same type.
     *
     * @param serviceName  the service name
     * @param memberUUID   the target member UUID
     * @param vectorClocks the vector clock map to set
     * @see CRDTReplicationAwareService
     */
    public void setReplicatedVectorClocks(String serviceName, UUID memberUUID, Map<String, VectorClock> vectorClocks) {
        replicatedVectorClocks.put(new ReplicatedVectorClockId(serviceName, memberUUID),
                Collections.unmodifiableMap(vectorClocks));
    }

    /**
     * Returns the vector clock map for the given {@code serviceName}.
     * For each CRDT belonging to that service, the map contains the latest
     * successfully replicated vector clocks to any replica.
     *
     * @param serviceName the CRDT service name
     * @return the last successfully replicated vector clock for all CRDTs for
     * the given {@code serviceName}
     * @see CRDTReplicationAwareService
     */
    public Map<String, VectorClock> getLatestReplicatedVectorClock(String serviceName) {
        final HashMap<String, VectorClock> latestVectorClocks = new HashMap<>();

        for (Entry<ReplicatedVectorClockId, Map<String, VectorClock>> clockEntry : replicatedVectorClocks.entrySet()) {
            final ReplicatedVectorClockId id = clockEntry.getKey();
            final Map<String, VectorClock> clock = clockEntry.getValue();

            if (id.serviceName.equals(serviceName)) {
                for (Entry<String, VectorClock> crdtReplicatedClocks : clock.entrySet()) {
                    final String crdtName = crdtReplicatedClocks.getKey();
                    final VectorClock vectorClock = crdtReplicatedClocks.getValue();
                    final VectorClock latestVectorClock = latestVectorClocks.get(crdtName);

                    if (latestVectorClock == null || vectorClock.isAfter(latestVectorClock)) {
                        latestVectorClocks.put(crdtName, vectorClock);
                    }
                }
            }
        }
        return latestVectorClocks;
    }

    /**
     * An identifier for a CRDT vector clock map. The clock is identified by
     * the CRDT service name and the target member UUID.
     *
     * @see CRDTReplicationAwareService
     */
    private static class ReplicatedVectorClockId {
        final UUID memberUUID;
        final String serviceName;

        ReplicatedVectorClockId(String serviceName, UUID memberUUID) {
            this.serviceName = checkNotNull(serviceName, "Service name must not be null");
            this.memberUUID = checkNotNull(memberUUID, "Member UUID must not be null");
        }

        @Override
        @SuppressWarnings("checkstyle:innerassignment")
        public boolean equals(Object o) {
            final ReplicatedVectorClockId that;
            return this == o || o instanceof ReplicatedVectorClockId
                    && this.serviceName.equals((that = (ReplicatedVectorClockId) o).serviceName)
                    && this.memberUUID.equals(that.memberUUID);
        }

        @Override
        public int hashCode() {
            int result = memberUUID.hashCode();
            result = 31 * result + serviceName.hashCode();
            return result;
        }
    }
}

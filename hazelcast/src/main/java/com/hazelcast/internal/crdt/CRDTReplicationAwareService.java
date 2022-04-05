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

import java.util.Map;

/**
 * Represents a service implementing a CRDT that may be replicated and
 * migrated to other replicas of the same CRDT. The service may optimize the
 * replication operation contents depending on the provided vector clocks.
 * The vector clock map contains mappings from CRDT name to the last
 * successfully replicated CRDT state version. All CRDTs in this map should
 * be of the same type.
 *
 * @param <T> CRDT implementation type
 */
public interface CRDTReplicationAwareService<T> {
    /**
     * Returns a replication operation for the provided vector clocks.
     * The vector clock map contains mappings from CRDT name to the last
     * successfully replicated CRDT vector clock. All CRDTs in this map should
     * be of the same type.
     * A return value of {@code null} means that there should not be any
     * replication operation.
     *
     * @param lastReplicatedVectorClock last successfully replicated vector clock
     * @param targetIndex               the index of the replication target in
     *                                  the membership list containing only data members
     * @return the replication operation and the replicated vector clocks
     */
    CRDTReplicationContainer prepareReplicationOperation(Map<String, VectorClock> lastReplicatedVectorClock, int targetIndex);

    /** Returns the name of the service */
    String getName();

    /**
     * Performs a merge of the local {@code name} CRDT with the provided state.
     *
     * @param name  the CRDT name
     * @param value the CRDT state to merge into the local state
     */
    void merge(String name, T value);

    /**
     * Returns a migration operation for all of the CRDT states not owned by
     * this member.
     *
     * @param maxConfiguredReplicaCount the maximum configured replica count
     *                                  for the CRDTs to be migrated (excluding)
     * @return the migration operation and the current vector clocks
     */
    CRDTReplicationContainer prepareMigrationOperation(int maxConfiguredReplicaCount);

    /**
     * Removes all of the CRDTs with vector clocks equal to the provided vector
     * clocks.
     * The vector clock map contains mappings from CRDT name to a vector clock.
     * All CRDTs in this map should be of the same type.
     *
     * @param vectorClocks a map from CRDT name to a vector clock
     * @return {@code true} if all of the CRDT states have been cleared,
     * {@code false} otherwise
     */
    boolean clearCRDTState(Map<String, VectorClock> vectorClocks);

    /**
     * Prepares the service for replication and shutdown. After this method has
     * returned, no mutations must be allowed to any existing CRDT state in
     * this service and new CRDT states must be disallowed.
     */
    void prepareToSafeShutdown();
}

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

/**
 * Common interface for CRDT (conflict-free replicated data type)
 * implementations.
 * The common behaviour for all implementations is that they have a strong
 * eventual consistency property - two nodes that have received the same
 * set of updates, possibly in a different order, will have an identical
 * shared state and any conflicting updates are merged automatically.
 * All operations on the CRDT implementations are performed locally without
 * any coordination with the other replicas of the same data. This allows
 * for higher availability but can present data loss in some scenarios,
 * e.g. the node is killed right after it receives an update. In split
 * brain scenarios, each replica should keep it's local state and once the
 * split brain is healed, the state of the replicas that were unreachable
 * during split brain as well as the local state will converge to the same
 * state.
 * The CRDT state can be versioned in which case it will increase the
 * version number on each mutation. This way, the replication mechanism
 * will be able to replicate only those states which have not yet been
 * replicated and avoid replication in a quiescent state.
 *
 * @param <T>
 */
public interface CRDT<T extends CRDT<T>> {
    /**
     * Merges the state of a different replica into this CRDT. The merge method
     * should not leave the CRDT in a failed state.
     *
     * @param other the CRDT to merge into this CRDT
     */
    void merge(T other);

    /**
     * Returns the current vector clock which reflects the current CRDT state.
     */
    VectorClock getCurrentVectorClock();
}

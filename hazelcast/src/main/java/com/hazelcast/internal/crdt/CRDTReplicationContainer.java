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
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Map;

/**
 * A container for a CRDT replication operation. It addition to the
 * operation, it contains additional information that may allow a specific
 * CRDT implementation service to optimise further replication operation
 * contents. For instance, the next replication operation may choose to
 * replicate only CRDT states which have changed from the previous
 * replication operation.
 */
public class CRDTReplicationContainer {
    private final Operation operation;
    private final Map<String, VectorClock> vectorClocks;

    public CRDTReplicationContainer(Operation operation, Map<String, VectorClock> vectorClocks) {
        this.operation = operation;
        this.vectorClocks = vectorClocks;
    }

    /**
     * Returns the CRDT replication operation.
     */
    public Operation getOperation() {
        return operation;
    }

    /**
     * Returns the vector clocks for the CRDTs replicated by the replication
     * operation.
     */
    public Map<String, VectorClock> getVectorClocks() {
        return vectorClocks;
    }
}

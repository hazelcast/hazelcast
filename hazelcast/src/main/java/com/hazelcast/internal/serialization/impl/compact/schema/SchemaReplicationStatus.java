/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.schema;

/**
 * Describes the status of the schema replication for a particular
 * schema id.
 */
public enum SchemaReplicationStatus {
    /**
     * The schema is available in the in-memory registry of the member and
     * persisted to the HotRestart (if enabled).
     */
    PREPARED(0),

    /**
     * The schema is known to be available and prepared for the all
     * cluster members.
     */
    REPLICATED(1);

    private final int id;

    SchemaReplicationStatus(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static SchemaReplicationStatus fromId(int id) {
        if (id == 0) {
            return PREPARED;
        } else if (id == 1) {
            return REPLICATED;
        } else {
            throw new IllegalStateException("Unknown id for SchemaReplicationStatus: " + id);
        }
    }
}

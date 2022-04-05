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

package com.hazelcast.config;

/**
 * Strategy for checking the consistency of data between replicas.
 *
 * @since 3.11
 */
public enum ConsistencyCheckStrategy {
    /**
     * No consistency checks
     */
    NONE((byte) 0),
    /**
     * Uses merkle trees (if configured) for consistency checks.
     *
     * @see MerkleTreeConfig
     */
    MERKLE_TREES((byte) 1);

    private static final ConsistencyCheckStrategy[] VALUES = values();
    private final byte id;

    ConsistencyCheckStrategy(byte id) {
        this.id = id;
    }

    /**
     * Gets the ID for this ConsistencyCheckStrategy.
     * <p>
     * The reason this ID is used instead of an the ordinal value is that the
     * ordinal value is more prone to changes due to reordering.
     *
     * @return the ID
     */
    public byte getId() {
        return id;
    }

    /**
     * Returns the ConsistencyCheckStrategy for the given ID.
     *
     * @return the ConsistencyCheckStrategy for the given ID
     * @throws IllegalArgumentException if no ConsistencyCheckStrategy was found
     */
    public static ConsistencyCheckStrategy getById(byte id) {
        for (ConsistencyCheckStrategy type : VALUES) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("Could not find a ConsistencyCheckStrategy with an ID:" + id);
    }
}

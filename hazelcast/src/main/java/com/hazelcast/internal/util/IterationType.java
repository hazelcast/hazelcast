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

package com.hazelcast.internal.util;

/**
 * To differentiate users selection on result collection on map-wide operations like values, keySet, query etc.
 */
public enum IterationType {

    /**
     * Iterate over keys
     */
    KEY((byte) 0),
    /**
     * Iterate over values
     */
    VALUE((byte) 1),
    /**
     * Iterate over whole entry (so key and value)
     */
    ENTRY((byte) 2);

    private static final IterationType[] VALUES = values();

    private final byte id;

    IterationType(byte id) {
        this.id = id;
    }

    /**
     * Gets the ID for the given IterationType.
     *
     * The reason this ID is used instead of an the ordinal value is that the ordinal value is more prone to changes due to
     * reordering.
     *
     * @return the ID
     */
    public byte getId() {
        return id;
    }

    /**
     * Returns the IterationType for the given ID.
     *
     * @return the IterationType for the given ID
     * @throws IllegalArgumentException if no IterationType was found
     */
    public static IterationType getById(byte id) {
        for (IterationType type : VALUES) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("unknown id:" + id);
    }
}

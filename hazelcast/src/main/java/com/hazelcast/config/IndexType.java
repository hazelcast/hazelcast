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
 * Type of the index.
 */
public enum IndexType {
    /** Sorted index. Can be used with equality and range predicates. */
    SORTED(0),

    /** Hash index. Can be used with equality predicates. */
    HASH(1),

    /** Bitmap index. Can be used with equality predicates. */
    BITMAP(2);

    private final int id;

    IndexType(int id) {
        this.id = id;
    }

    /**
     * Gets the ID for the given {@link IndexType}.
     *
     * @return the ID
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the IndexType as an enum.
     *
     * @return the IndexType as an enum
     */
    public static IndexType getById(final int id) {
        for (IndexType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        return null;
    }
}

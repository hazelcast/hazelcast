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
 * Storage format type of values stored in cluster
 */
public enum InMemoryFormat {
    /**
     * As Binary
     */
    BINARY(0),
    /**
     * As Object
     */
    OBJECT(1),

    /**
     * As native storage
     */
    NATIVE(2);

    private final byte id;

    InMemoryFormat(int id) {
        this.id = (byte) id;
    }

    public byte getId() {
        return id;
    }

    public static InMemoryFormat getById(int id) {
        for (InMemoryFormat imf : values()) {
            if (imf.getId() == id) {
                return imf;
            }
        }
        throw new IllegalArgumentException("Unsupported ID value");
    }
}

/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
 * Eviction Policy enum.
 */
public enum EvictionPolicy {
    /**
     * Least Recently Used
     */
    LRU(0),
    /**
     * Least Frequently Used
     */
    LFU(1),
    /**
     * None
     */
    NONE(2),
    /**
     * Randomly
     */
    RANDOM(3);

    private final byte id;

    EvictionPolicy(int id) {
        this.id = (byte) id;
    }

    public byte getId() {
        return id;
    }

    public static EvictionPolicy getById(int id) {
        for (EvictionPolicy ep : values()) {
            if (ep.getId() == id) {
                return ep;
            }
        }
        throw new IllegalArgumentException("Unsupported ID value");
    }
}

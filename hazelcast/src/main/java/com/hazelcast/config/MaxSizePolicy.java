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
 * Maximum Size Policy
 */
public enum MaxSizePolicy {
    /**
     * Policy based on maximum number of entries stored per data
     * structure (map, cache etc) on each Hazelcast instance
     */
    PER_NODE(0),
    /**
     * Policy based on maximum number of entries stored per
     * data structure (map, cache etc) on each partition
     */
    PER_PARTITION(1),
    /**
     * Policy based on maximum used JVM heap memory percentage per
     * data structure (map, cache etc) on each Hazelcast instance
     */
    USED_HEAP_PERCENTAGE(2),
    /**
     * Policy based on maximum used JVM heap memory in megabytes per
     * data structure (map, cache etc) on each Hazelcast instance
     */
    USED_HEAP_SIZE(3),
    /**
     * Policy based on minimum free JVM
     * heap memory percentage per JVM
     */
    FREE_HEAP_PERCENTAGE(4),
    /**
     * Policy based on minimum free JVM
     * heap memory in megabytes per JVM
     */
    FREE_HEAP_SIZE(5),
    /**
     * Policy based on maximum number of entries
     * stored per data structure (map, cache etc)
     */
    ENTRY_COUNT(6),
    /**
     * Policy based on maximum used native memory in megabytes per
     * data structure (map, cache etc) on each Hazelcast instance
     */
    USED_NATIVE_MEMORY_SIZE(7),
    /**
     * Policy based on maximum used native memory percentage per
     * data structure (map, cache etc) on each Hazelcast instance
     */
    USED_NATIVE_MEMORY_PERCENTAGE(8),
    /**
     * Policy based on minimum free native
     * memory in megabytes per Hazelcast instance
     */
    FREE_NATIVE_MEMORY_SIZE(9),
    /**
     * Policy based on minimum free native
     * memory percentage per Hazelcast instance
     */
    FREE_NATIVE_MEMORY_PERCENTAGE(10);

    private static final MaxSizePolicy[] VALUES = values();

    private final int id;

    MaxSizePolicy(int id) {
        this.id = (byte) id;
    }

    public int getId() {
        return id;
    }

    public static MaxSizePolicy getById(int id) {
        return VALUES[id];
    }
}

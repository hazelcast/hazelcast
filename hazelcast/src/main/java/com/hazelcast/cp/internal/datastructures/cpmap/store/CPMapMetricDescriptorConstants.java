/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.store;

/**
 * Defines metric descriptor names for {@link com.hazelcast.cp.CPMap}.
 * <p>
 * All constants in this class represent logical metric identifiers and are
 * consumed by the Hazelcast metrics subsystem. Naming follows a stable,
 * hierarchical convention to ensure backward compatibility.
 */
public final class CPMapMetricDescriptorConstants {

    /**
     * Root prefix for all CPMap-related metrics.
     */
    public static final String CP_MAP_ROOT_PREFIX = "cp.map";

    /**
     * Summary metric group for CPMap.
     */
    public static final String CP_MAP_SUMMARY = CP_MAP_ROOT_PREFIX + ".summary";

    // --- Size-related metrics ---

    /**
     * Total number of entries in the CPMap.
     */
    public static final String SIZE = "size";

    /**
     * Heap memory footprint of CPMap entries, in bytes.
     */
    public static final String SIZE_BYTES = "sizeBytes";

    // --- Purge-related metrics ---

    /**
     * Root prefix for purge-related metrics.
     */
    public static final String PURGE_ROOT_PREFIX = "purge";

    /**
     * Number of entries removed in the most recent purge operation.
     */
    public static final String PURGE_LAST_COUNT =
            PURGE_ROOT_PREFIX + ".last.count";

    /**
     * Duration of the most recent purge operation, in nanoseconds.
     */
    public static final String PURGE_LAST_DURATION_NANOSECONDS =
            PURGE_ROOT_PREFIX + ".last.duration.nanoseconds";

    /**
     * Maximum observed purge duration since startup, in nanoseconds.
     */
    public static final String PURGE_MAX_DURATION_NANOSECONDS =
            PURGE_ROOT_PREFIX + ".max.duration.nanoseconds";

    /**
     * Cumulative number of entries purged since startup.
     */
    public static final String PURGE_TOTAL_COUNT =
            PURGE_ROOT_PREFIX + ".total.count";

    /**
     * Number of entries currently eligible for purging.
     */
    public static final String PURGE_PENDING_COUNT =
            PURGE_ROOT_PREFIX + ".pending.count";

    private CPMapMetricDescriptorConstants() {
        // Utility class; not instantiable
    }
}

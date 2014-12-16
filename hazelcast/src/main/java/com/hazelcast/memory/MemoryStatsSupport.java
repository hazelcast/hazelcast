/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.memory;

import static com.hazelcast.util.OperatingSystemMXBeanSupport.readLongAttribute;

/**
 * This class provides heap usage statistics
 *
 */
public final class MemoryStatsSupport {

    private static final long TOTAL_PHYSICAL_MEMORY = readLongAttribute("TotalPhysicalMemorySize", -1L);

    private static final long TOTAL_SWAP_SPACE = readLongAttribute("TotalSwapSpaceSize", -1L);

    /**
     * No public constructor is needed for utility classes
     */
    private MemoryStatsSupport() { }

    public static long totalPhysicalMemory() {
        return TOTAL_PHYSICAL_MEMORY;
    }

    public static long freePhysicalMemory() {
        return readLongAttribute("FreePhysicalMemorySize", -1L);
    }

    public static long totalSwapSpace() {
        return TOTAL_SWAP_SPACE;
    }

    public static long freeSwapSpace() {
        return readLongAttribute("FreeSwapSpaceSize", -1L);
    }

}

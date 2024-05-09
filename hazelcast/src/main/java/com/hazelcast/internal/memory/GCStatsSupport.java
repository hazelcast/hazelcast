/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.memory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * Used to gather garbage collection statistics.
 */
public final class GCStatsSupport {

    public static final Set<String> MINOR_GC;
    public static final Set<String> MAJOR_GC;

    static {
        final Set<String> minorGC = createHashSet(8);
        // Hotspot JREs
        minorGC.add("PS Scavenge");
        minorGC.add("ParNew");
        minorGC.add("G1 Young Generation");
        minorGC.add("Copy");
        minorGC.add("ZGC");
        minorGC.add("Shenandoah Cycles");
        // ZGC Non-generational
        minorGC.add("ZGC Cycles");
        minorGC.add("ZGC Pauses");
        // ZGC Generational
        minorGC.add("ZGC Minor Cycles");
        minorGC.add("ZGC Minor Pauses");
        //IBM & OpenJ9 JREs
        minorGC.add("partial gc");
        minorGC.add("scavenge");
        MINOR_GC = Collections.unmodifiableSet(minorGC);

        final Set<String> majorGC = createHashSet(8);
        // Hotspot JREs
        majorGC.add("PS MarkSweep");
        majorGC.add("ConcurrentMarkSweep");
        majorGC.add("G1 Old Generation");
        majorGC.add("G1 Mixed Generation");
        majorGC.add("MarkSweepCompact");
        majorGC.add("Shenandoah Pauses");
        // ZGC Generational
        majorGC.add("ZGC Major Cycles");
        majorGC.add("ZGC Major Pauses");
        //IBM & OpenJ9 JREs
        majorGC.add("global");
        majorGC.add("global garbage collect");
        MAJOR_GC = Collections.unmodifiableSet(majorGC);
    }

    /**
     * No public constructor is needed for utility classes
     */
    private GCStatsSupport() {
    }

    static void fill(DefaultGarbageCollectorStats stats) {
        long minorCount = 0;
        long minorTime = 0;
        long majorCount = 0;
        long majorTime = 0;
        long unknownCount = 0;
        long unknownTime = 0;

        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = gc.getCollectionCount();
            if (count >= 0) {
                if (MINOR_GC.contains(gc.getName())) {
                    minorCount += count;
                    minorTime += gc.getCollectionTime();
                } else if (MAJOR_GC.contains(gc.getName())) {
                    majorCount += count;
                    majorTime += gc.getCollectionTime();
                } else {
                    unknownCount += count;
                    unknownTime += gc.getCollectionTime();
                }
            }
        }

        stats.setMajorCount(majorCount);
        stats.setMajorTime(majorTime);
        stats.setMinorCount(minorCount);
        stats.setMinorTime(minorTime);
        stats.setUnknownCount(unknownCount);
        stats.setUnknownTime(unknownTime);
    }

    public static GarbageCollectorStats getGCStats() {
        DefaultGarbageCollectorStats stats = new DefaultGarbageCollectorStats();
        fill(stats);
        return stats;
    }
}

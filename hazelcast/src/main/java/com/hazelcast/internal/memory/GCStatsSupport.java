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

    public static final Set<String> YOUNG_GC;
    public static final Set<String> OLD_GC;

    static {
        final Set<String> youngGC = createHashSet(8);
        // Hotspot JREs
        youngGC.add("PS Scavenge");
        youngGC.add("ParNew");
        youngGC.add("G1 Young Generation");
        youngGC.add("Copy");
        youngGC.add("ZGC");
        youngGC.add("Shenandoah Cycles");
        //IBM & OpenJ9 JREs
        youngGC.add("partial gc");
        youngGC.add("scavenge");
        YOUNG_GC = Collections.unmodifiableSet(youngGC);

        final Set<String> oldGC = createHashSet(8);
        // Hotspot JREs
        oldGC.add("PS MarkSweep");
        oldGC.add("ConcurrentMarkSweep");
        oldGC.add("G1 Old Generation");
        oldGC.add("G1 Mixed Generation");
        oldGC.add("MarkSweepCompact");
        oldGC.add("Shenandoah Pauses");
        //IBM & OpenJ9 JREs
        oldGC.add("global");
        oldGC.add("global garbage collect");
        OLD_GC = Collections.unmodifiableSet(oldGC);
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
                if (YOUNG_GC.contains(gc.getName())) {
                    minorCount += count;
                    minorTime += gc.getCollectionTime();
                } else if (OLD_GC.contains(gc.getName())) {
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

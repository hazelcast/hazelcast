/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.metricsets;


import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeName;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A metrics set for exposing {@link GarbageCollectorMXBean} metrics.
 */
public final class GarbageCollectionMetricSet {

    private static final Set<String> YOUNG_GC = new HashSet<String>(3);
    private static final Set<String> OLD_GC = new HashSet<String>(3);

    static {
        YOUNG_GC.add("PS Scavenge");
        YOUNG_GC.add("ParNew");
        YOUNG_GC.add("G1 Young Generation");

        OLD_GC.add("PS MarkSweep");
        OLD_GC.add("ConcurrentMarkSweep");
        OLD_GC.add("G1 Old Generation");
    }

    private GarbageCollectionMetricSet() {
    }

    /**
     * Registers all the metrics in this metrics pack.
     *
     * @param metricsRegistry the MetricsRegister upon which the metrics are registered.
     */
    public static void register(MetricsRegistry metricsRegistry) {
        checkNotNull(metricsRegistry, "metricsRegistry");
        metricsRegistry.registerRoot(new GcProbes());
    }


    @SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "used by instrumentation tools")
    static final class GcProbes {
        static final int EXPIRATION_MS = 100;

        volatile long lastUpdateMs;

        volatile long minorCount;
        volatile long minorTime;
        volatile long majorCount;
        volatile long majorTime;
        volatile long unknownCount;
        volatile long unknownTime;

        void update() {
            long nowMs = System.currentTimeMillis();

            if (lastUpdateMs + EXPIRATION_MS > nowMs) {
                // this isn't thread safe since multiple thread could decide at the same time to update. But that it will only
                // lead to multiple concurrent updates of the fields; which doesn't lead to incorrect results. This is cheaper
                // than relying on locks.
                return;
            }

            long minorCount = 0;
            long minorTime = 0;
            long majorCount = 0;
            long majorTime = 0;
            long unknownCount = 0;
            long unknownTime = 0;

            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                long count = gc.getCollectionCount();
                if (count == -1) {
                    continue;
                }

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

            this.minorCount = minorCount;
            this.minorTime = minorTime;
            this.majorCount = majorCount;
            this.majorTime = majorTime;
            this.unknownCount = unknownCount;
            this.unknownTime = unknownTime;

            lastUpdateMs = nowMs;
        }

        @Probe
        long minorCount() {
            update();
            return minorCount;
        }

        @Probe
        long minorTime() {
            update();
            return minorTime;
        }

        @Probe
        long majorCount() {
            update();
            return majorCount;
        }

        @Probe
        long majorTime() {
            update();
            return majorTime;
        }

        @Probe
        long unknownCount() {
            update();
            return unknownCount;
        }

        @Probe
        long unknownTime() {
            update();
            return unknownTime;
        }

        @ProbeName
        String probeName() {
            return "gc";
        }
    }
}

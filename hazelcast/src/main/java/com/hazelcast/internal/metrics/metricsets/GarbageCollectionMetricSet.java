/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.SetUtil.createHashSet;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A metrics set for exposing {@link GarbageCollectorMXBean} metrics.
 */
public final class GarbageCollectionMetricSet {

    private static final Set<String> YOUNG_GC;
    private static final Set<String> OLD_GC;
    private static final int PUBLISH_FREQUENCY_SECONDS = 1;

    static {
        final Set<String> youngGC = createHashSet(3);
        youngGC.add("PS Scavenge");
        youngGC.add("ParNew");
        youngGC.add("G1 Young Generation");
        YOUNG_GC = Collections.unmodifiableSet(youngGC);

        final Set<String> oldGC = createHashSet(3);
        oldGC.add("PS MarkSweep");
        oldGC.add("ConcurrentMarkSweep");
        oldGC.add("G1 Old Generation");
        OLD_GC = Collections.unmodifiableSet(oldGC);
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

        GcStats stats = new GcStats();
        metricsRegistry.scheduleAtFixedRate(stats, PUBLISH_FREQUENCY_SECONDS, SECONDS);
        metricsRegistry.scanAndRegister(stats, "gc");
    }


    @SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "used by instrumentation tools")
    static class GcStats implements Runnable {
        @Probe(level = MANDATORY)
        volatile long minorCount;
        @Probe(level = MANDATORY)
        volatile long minorTime;
        @Probe(level = MANDATORY)
        volatile long majorCount;
        @Probe(level = MANDATORY)
        volatile long majorTime;
        @Probe(level = MANDATORY)
        volatile long unknownCount;
        @Probe(level = MANDATORY)
        volatile long unknownTime;

        @Override
        public void run() {
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
        }
    }
}

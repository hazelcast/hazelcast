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

package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.memory.GCStatsSupport;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.GC_METRIC_MAJOR_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.GC_METRIC_MAJOR_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.GC_METRIC_MINOR_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.GC_METRIC_MINOR_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.GC_METRIC_UNKNOWN_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.GC_METRIC_UNKNOWN_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.GC_PREFIX;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A metrics set for exposing {@link GarbageCollectorMXBean} metrics.
 */
public final class GarbageCollectionMetricSet {

    private static final int PUBLISH_FREQUENCY_SECONDS = 1;

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
        metricsRegistry.scheduleAtFixedRate(stats, PUBLISH_FREQUENCY_SECONDS, SECONDS, INFO);
        metricsRegistry.registerStaticMetrics(stats, GC_PREFIX);
    }


    @SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "used by instrumentation tools")
    static class GcStats implements Runnable {
        @Probe(name = GC_METRIC_MINOR_COUNT, level = MANDATORY)
        volatile long minorCount;
        @Probe(name = GC_METRIC_MINOR_TIME, unit = MS, level = MANDATORY)
        volatile long minorTime;
        @Probe(name = GC_METRIC_MAJOR_COUNT, level = MANDATORY)
        volatile long majorCount;
        @Probe(name = GC_METRIC_MAJOR_TIME, unit = MS, level = MANDATORY)
        volatile long majorTime;
        @Probe(name = GC_METRIC_UNKNOWN_COUNT, level = MANDATORY)
        volatile long unknownCount;
        @Probe(name = GC_METRIC_UNKNOWN_TIME, unit = MS, level = MANDATORY)
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

                if (GCStatsSupport.YOUNG_GC.contains(gc.getName())) {
                    minorCount += count;
                    minorTime += gc.getCollectionTime();
                } else if (GCStatsSupport.OLD_GC.contains(gc.getName())) {
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

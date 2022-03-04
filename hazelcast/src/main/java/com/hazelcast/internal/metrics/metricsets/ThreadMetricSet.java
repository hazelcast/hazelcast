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

import com.hazelcast.internal.metrics.MetricsRegistry;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.THREAD_FULL_METRIC_DAEMON_THREAD_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.THREAD_FULL_METRIC_PEAK_THREAD_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.THREAD_FULL_METRIC_THREAD_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.THREAD_FULL_METRIC_TOTAL_STARTED_THREAD_COUNT;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * A Metric pack for exposing {@link ThreadMXBean} metric.
 */
public final class ThreadMetricSet {

    private ThreadMetricSet() {
    }

    /**
     * Registers all the metrics in this metric pack.
     *
     * @param metricsRegistry the MetricsRegistry upon which the metrics are registered.
     */
    public static void register(MetricsRegistry metricsRegistry) {
        checkNotNull(metricsRegistry, "metricsRegistry");

        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        metricsRegistry.registerStaticProbe(mxBean, THREAD_FULL_METRIC_THREAD_COUNT, MANDATORY, ThreadMXBean::getThreadCount);
        metricsRegistry
                .registerStaticProbe(mxBean, THREAD_FULL_METRIC_PEAK_THREAD_COUNT, MANDATORY, ThreadMXBean::getPeakThreadCount);
        metricsRegistry.registerStaticProbe(mxBean, THREAD_FULL_METRIC_DAEMON_THREAD_COUNT, MANDATORY,
                ThreadMXBean::getDaemonThreadCount);
        metricsRegistry.registerStaticProbe(mxBean, THREAD_FULL_METRIC_TOTAL_STARTED_THREAD_COUNT, MANDATORY,
                ThreadMXBean::getTotalStartedThreadCount);
    }
}

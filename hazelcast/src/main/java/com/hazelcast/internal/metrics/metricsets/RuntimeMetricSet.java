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
import com.hazelcast.internal.util.JVMUtil;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.RUNTIME_FULL_METRIC_AVAILABLE_PROCESSORS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.RUNTIME_FULL_METRIC_FREE_MEMORY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.RUNTIME_FULL_METRIC_MAX_MEMORY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.RUNTIME_FULL_METRIC_TOTAL_MEMORY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.RUNTIME_FULL_METRIC_UPTIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.RUNTIME_FULL_METRIC_USED_MEMORY;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * A Metric pack for exposing {@link java.lang.Runtime} metrics.
 */
public final class RuntimeMetricSet {

    private RuntimeMetricSet() {
    }

    /**
     * Registers all the metrics in this metrics pack.
     *
     * @param metricsRegistry the MetricsRegistry upon which the metrics are registered.
     */
    public static void register(MetricsRegistry metricsRegistry) {
        checkNotNull(metricsRegistry, "metricsRegistry");

        Runtime runtime = Runtime.getRuntime();
        RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();

        metricsRegistry.registerStaticProbe(runtime, RUNTIME_FULL_METRIC_FREE_MEMORY, MANDATORY, Runtime::freeMemory);
        metricsRegistry.registerStaticProbe(runtime, RUNTIME_FULL_METRIC_TOTAL_MEMORY, MANDATORY, Runtime::totalMemory);
        metricsRegistry.registerStaticProbe(runtime, RUNTIME_FULL_METRIC_MAX_MEMORY, MANDATORY, Runtime::maxMemory);
        metricsRegistry.registerStaticProbe(runtime, RUNTIME_FULL_METRIC_USED_MEMORY, MANDATORY, JVMUtil::usedMemory);
        metricsRegistry
                .registerStaticProbe(runtime, RUNTIME_FULL_METRIC_AVAILABLE_PROCESSORS, MANDATORY, Runtime::availableProcessors);
        metricsRegistry.registerStaticProbe(mxBean, RUNTIME_FULL_METRIC_UPTIME, MANDATORY, RuntimeMXBean::getUptime);
    }
}

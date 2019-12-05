/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

        metricsRegistry.registerStaticProbe(runtime, "runtime.freeMemory", MANDATORY, Runtime::freeMemory);
        metricsRegistry.registerStaticProbe(runtime, "runtime.totalMemory", MANDATORY, Runtime::totalMemory);
        metricsRegistry.registerStaticProbe(runtime, "runtime.maxMemory", MANDATORY, Runtime::maxMemory);
        metricsRegistry.registerStaticProbe(runtime, "runtime.usedMemory", MANDATORY, JVMUtil::usedMemory);
        metricsRegistry.registerStaticProbe(runtime, "runtime.availableProcessors", MANDATORY, Runtime::availableProcessors);
        metricsRegistry.registerStaticProbe(mxBean, "runtime.uptime", MANDATORY, RuntimeMXBean::getUptime);
    }
}

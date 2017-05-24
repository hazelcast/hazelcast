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

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A Metric pack for exposing {@link java.lang.Runtime} metrics.
 */
public final class RuntimeMetricSet {
    private static final Set<String> registeredMetrics = new HashSet<String>();

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

        metricsRegistry.register(runtime, "runtime.freeMemory", MANDATORY,
                new LongProbeFunction<Runtime>() {
                    @Override
                    public long get(Runtime runtime) {
                        return runtime.freeMemory();
                    }
                }
        );
        registeredMetrics.add("runtime.freeMemory");

        metricsRegistry.register(runtime, "runtime.totalMemory", MANDATORY,
                new LongProbeFunction<Runtime>() {
                    @Override
                    public long get(Runtime runtime) {
                        return runtime.totalMemory();
                    }
                }
        );
        registeredMetrics.add("runtime.totalMemory");

        metricsRegistry.register(runtime, "runtime.maxMemory", MANDATORY,
                new LongProbeFunction<Runtime>() {
                    @Override
                    public long get(Runtime runtime) {
                        return runtime.maxMemory();
                    }
                }
        );
        registeredMetrics.add("runtime.maxMemory");

        metricsRegistry.register(runtime, "runtime.usedMemory", MANDATORY,
                new LongProbeFunction<Runtime>() {
                    @Override
                    public long get(Runtime runtime) {
                        return runtime.totalMemory() - runtime.freeMemory();
                    }
                }
        );
        registeredMetrics.add("runtime.usedMemory");

        metricsRegistry.register(runtime, "runtime.availableProcessors", MANDATORY,
                new LongProbeFunction<Runtime>() {
                    @Override
                    public long get(Runtime runtime) {
                        return runtime.availableProcessors();
                    }
                }
        );
        registeredMetrics.add("runtime.availableProcessors");

        metricsRegistry.register(mxBean, "runtime.uptime", MANDATORY,
                new LongProbeFunction<RuntimeMXBean>() {
                    @Override
                    public long get(RuntimeMXBean runtimeMXBean) {
                        return runtimeMXBean.getUptime();
                    }
                }
        );
        registeredMetrics.add("runtime.uptime");
    }

    public static Set<String> getRegisteredMetricNames() {
        return Collections.unmodifiableSet(registeredMetrics);
    }
}

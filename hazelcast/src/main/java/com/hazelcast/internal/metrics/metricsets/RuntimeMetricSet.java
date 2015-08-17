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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A Metric pack for exposing {@link java.lang.Runtime} metrics.
 */
public final class RuntimeMetricSet {

    private RuntimeMetricSet() {
    }

    /**
     * Registers all the metrics in this metrics pack.
     *how is
     * @param metricsRegistry the MetricsRegistry upon which the metrics are registered.
     */
    public static void register(MetricsRegistry metricsRegistry) {
        checkNotNull(metricsRegistry, "metricsRegistry can't be null");
        metricsRegistry.registerRoot(new RuntimeProbes());
    }

    private static final class RuntimeProbes {
        private final Runtime runtime = Runtime.getRuntime();
        private final RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();

        @Probe
        long freeMemory() {
            return runtime.freeMemory();
        }

        @Probe
        long totalMemory() {
            return runtime.totalMemory();
        }

        @Probe
        long maxMemory() {
            return runtime.maxMemory();
        }

        @Probe
        long usedMemory() {
            return runtime.totalMemory() - runtime.freeMemory();
        }

        @Probe
        int availableProcessors() {
            return runtime.availableProcessors();
        }

        @Probe
        long upTime() {
            return mxBean.getUptime();
        }

        @ProbeName
        String probeName() {
            return "runtime";
        }
    }
}

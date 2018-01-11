/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A Metric set for exposing {@link java.lang.management.ClassLoadingMXBean} metrics.
 */
public final class ClassLoadingMetricSet {

    private ClassLoadingMetricSet() {
    }

    /**
     * Registers all the metrics in this metric pack.
     *
     * @param metricsRegistry the MetricsRegistry upon which the metrics are registered.
     */
    public static void register(MetricsRegistry metricsRegistry) {
        checkNotNull(metricsRegistry, "metricsRegistry");

        ClassLoadingMXBean mxBean = ManagementFactory.getClassLoadingMXBean();

        metricsRegistry.register(mxBean, "classloading.loadedClassesCount", MANDATORY,
                new LongProbeFunction<ClassLoadingMXBean>() {
                    @Override
                    public long get(ClassLoadingMXBean classLoadingMXBean) {
                        return classLoadingMXBean.getLoadedClassCount();
                    }
                }
        );

        metricsRegistry.register(mxBean, "classloading.totalLoadedClassesCount", MANDATORY,
                new LongProbeFunction<ClassLoadingMXBean>() {
                    @Override
                    public long get(ClassLoadingMXBean classLoadingMXBean) {
                        return classLoadingMXBean.getTotalLoadedClassCount();
                    }
                }
        );

        metricsRegistry.register(mxBean, "classloading.unloadedClassCount", MANDATORY,
                new LongProbeFunction<ClassLoadingMXBean>() {
                    @Override
                    public long get(ClassLoadingMXBean classLoadingMXBean) {
                        return classLoadingMXBean.getUnloadedClassCount();
                    }
                }
        );
    }
}

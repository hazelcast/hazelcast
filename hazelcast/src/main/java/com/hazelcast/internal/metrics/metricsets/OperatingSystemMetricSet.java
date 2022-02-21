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

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.util.OperatingSystemMXBeanSupport;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.logging.Level;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_COMMITTED_VIRTUAL_MEMORY_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_FREE_PHYSICAL_MEMORY_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_FREE_SWAP_SPACE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_MAX_FILE_DESCRIPTOR_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_OPEN_FILE_DESCRIPTOR_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_PROCESS_CPU_LOAD;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_PROCESS_CPU_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_SYSTEM_CPU_LOAD;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_SYSTEM_LOAD_AVERAGE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_TOTAL_PHYSICAL_MEMORY_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OS_FULL_METRIC_TOTAL_SWAP_SPACE_SIZE;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * A Metric set for exposing {@link java.lang.management.OperatingSystemMXBean} metrics.
 */
public final class OperatingSystemMetricSet {

    private static final ILogger LOGGER = Logger.getLogger(OperatingSystemMetricSet.class);
    private static final long PERCENTAGE_MULTIPLIER = 100;
    private static final Object[] EMPTY_ARGS = new Object[0];

    private OperatingSystemMetricSet() {
    }

    /**
     * Registers all the metrics in this metrics pack.
     *
     * @param metricsRegistry the MetricsRegistry upon which the metrics are registered.
     */
    public static void register(MetricsRegistry metricsRegistry) {
        checkNotNull(metricsRegistry, "metricsRegistry");

        OperatingSystemMXBean mxBean = ManagementFactory.getOperatingSystemMXBean();

        registerMethod(metricsRegistry, mxBean, "getCommittedVirtualMemorySize", OS_FULL_METRIC_COMMITTED_VIRTUAL_MEMORY_SIZE);
        registerMethod(metricsRegistry, mxBean, "getFreePhysicalMemorySize", OS_FULL_METRIC_FREE_PHYSICAL_MEMORY_SIZE);
        registerMethod(metricsRegistry, mxBean, "getFreeSwapSpaceSize", OS_FULL_METRIC_FREE_SWAP_SPACE_SIZE);
        registerMethod(metricsRegistry, mxBean, "getProcessCpuTime", OS_FULL_METRIC_PROCESS_CPU_TIME);
        registerMethod(metricsRegistry, mxBean, "getTotalPhysicalMemorySize", OS_FULL_METRIC_TOTAL_PHYSICAL_MEMORY_SIZE);
        registerMethod(metricsRegistry, mxBean, "getTotalSwapSpaceSize", OS_FULL_METRIC_TOTAL_SWAP_SPACE_SIZE);
        registerMethod(metricsRegistry, mxBean, "getMaxFileDescriptorCount", OS_FULL_METRIC_MAX_FILE_DESCRIPTOR_COUNT);
        registerMethod(metricsRegistry, mxBean, "getOpenFileDescriptorCount", OS_FULL_METRIC_OPEN_FILE_DESCRIPTOR_COUNT);

        // value will be between 0.0 and 1.0 or a negative value, if not available
        registerMethod(metricsRegistry, mxBean, "getProcessCpuLoad", OS_FULL_METRIC_PROCESS_CPU_LOAD, PERCENTAGE_MULTIPLIER);

        // value will be between 0.0 and 1.0 or a negative value, if not available
        registerMethod(metricsRegistry, mxBean, "getSystemCpuLoad", OS_FULL_METRIC_SYSTEM_CPU_LOAD, PERCENTAGE_MULTIPLIER);

        metricsRegistry.registerStaticProbe(mxBean, OS_FULL_METRIC_SYSTEM_LOAD_AVERAGE, MANDATORY,
                OperatingSystemMXBean::getSystemLoadAverage
        );
    }

    static void registerMethod(MetricsRegistry metricsRegistry, Object osBean, String methodName, String name) {
        if (OperatingSystemMXBeanSupport.GET_FREE_PHYSICAL_MEMORY_SIZE_DISABLED
                && methodName.equals("getFreePhysicalMemorySize")) {
            metricsRegistry.registerStaticProbe(osBean, name, MANDATORY, (LongProbeFunction<Object>) source -> -1);
        } else {
            registerMethod(metricsRegistry, osBean, methodName, name, 1);
        }
    }

    private static void registerMethod(MetricsRegistry metricsRegistry, Object osBean, String methodName, String name,
                                       final long multiplier) {
        final Method method = getMethod(osBean, methodName, name);

        if (method == null) {
            return;
        }

        if (long.class.equals(method.getReturnType())) {
            metricsRegistry.registerStaticProbe(osBean, name, MANDATORY,
                    (LongProbeFunction) bean -> (Long) method.invoke(bean, EMPTY_ARGS) * multiplier);
        } else {
            metricsRegistry.registerStaticProbe(osBean, name, MANDATORY,
                    (DoubleProbeFunction) bean -> (Double) method.invoke(bean, EMPTY_ARGS) * multiplier);
        }
    }

    /**
     * Returns a method from the given source object.
     *
     * @param source     the source object.
     * @param methodName the name of the method to retrieve.
     * @param name       the probe name
     * @return the method
     */
    private static Method getMethod(Object source, String methodName, String name) {
        try {
            Method method = source.getClass().getMethod(methodName);
            method.setAccessible(true);
            return method;
        } catch (Exception e) {
            if (LOGGER.isFinestEnabled()) {
                LOGGER.log(Level.FINEST,
                        "Unable to register OperatingSystemMXBean method " + methodName + " used for probe " + name, e);
            }
            return null;
        }
    }
}

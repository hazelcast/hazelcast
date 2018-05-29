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

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A Metric set for exposing {@link java.lang.management.OperatingSystemMXBean} metrics.
 */
public final class OperatingSystemMetricSet {

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

        registerMethod(metricsRegistry, mxBean, "getCommittedVirtualMemorySize", "os.committedVirtualMemorySize");
        registerMethod(metricsRegistry, mxBean, "getFreePhysicalMemorySize", "os.freePhysicalMemorySize");
        registerMethod(metricsRegistry, mxBean, "getFreeSwapSpaceSize", "os.freeSwapSpaceSize");
        registerMethod(metricsRegistry, mxBean, "getProcessCpuTime", "os.processCpuTime");
        registerMethod(metricsRegistry, mxBean, "getTotalPhysicalMemorySize", "os.totalPhysicalMemorySize");
        registerMethod(metricsRegistry, mxBean, "getTotalSwapSpaceSize", "os.totalSwapSpaceSize");
        registerMethod(metricsRegistry, mxBean, "getMaxFileDescriptorCount", "os.maxFileDescriptorCount");
        registerMethod(metricsRegistry, mxBean, "getOpenFileDescriptorCount", "os.openFileDescriptorCount");
        registerMethod(metricsRegistry, mxBean, "getProcessCpuLoad", "os.processCpuLoad", PERCENTAGE_MULTIPLIER);
        registerMethod(metricsRegistry, mxBean, "getSystemCpuLoad", "os.systemCpuLoad", PERCENTAGE_MULTIPLIER);

        metricsRegistry.register(mxBean, "os.systemLoadAverage", MANDATORY,
                new DoubleProbeFunction<OperatingSystemMXBean>() {
                    @Override
                    public double get(OperatingSystemMXBean bean) {
                        return bean.getSystemLoadAverage();
                    }
                }
        );
    }

    static void registerMethod(MetricsRegistry metricsRegistry, Object osBean, String methodName, String name) {
        registerMethod(metricsRegistry, osBean, methodName, name, 1);
    }

    private static void registerMethod(MetricsRegistry metricsRegistry, Object osBean, String methodName, String name,
                                       final long multiplier) {
        final Method method = getMethod(osBean, methodName);

        if (method == null) {
            return;
        }

        if (long.class.equals(method.getReturnType())) {
            metricsRegistry.register(osBean, name, MANDATORY, new LongProbeFunction() {
                @Override
                public long get(Object bean) throws Exception {
                    return (Long) method.invoke(bean, EMPTY_ARGS) * multiplier;
                }
            });
        } else {
            metricsRegistry.register(osBean, name, MANDATORY, new DoubleProbeFunction() {
                @Override
                public double get(Object bean) throws Exception {
                    return (Double) method.invoke(bean, EMPTY_ARGS) * multiplier;
                }
            });
        }
    }

    /**
     * Returns a method from the given source object.
     *
     * @param source     the source object.
     * @param methodName the name of the method to retrieve.
     * @return the method
     */
    private static Method getMethod(Object source, String methodName) {
        try {
            Method method = source.getClass().getDeclaredMethod(methodName);
            method.setAccessible(true);
            return method;
        } catch (Exception e) {
            return null;
        }
    }
}

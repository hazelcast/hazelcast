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
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A Metric set for exposing {@link java.lang.management.OperatingSystemMXBean} metrics.
 */
public final class OperatingSystemMetricsSet {

    private static final double PERCENTAGE_MULTIPLIER = 100d;

    private OperatingSystemMetricsSet() {
    }

    /**
     * Registers all the metrics in this metrics pack.
     *
     * @param metricsRegistry the MetricsRegistry upon which the metrics are registered.
     */
    public static void register(MetricsRegistry metricsRegistry) {
        checkNotNull(metricsRegistry, "metricsRegistry");
        metricsRegistry.registerRoot(new OperatingSystemProbes());
    }

    static final class OperatingSystemProbes {
        private final OperatingSystemMXBean mxBean = ManagementFactory.getOperatingSystemMXBean();
        private final Method getCommittedVirtualMemorySize = getMethod(mxBean, "getCommittedVirtualMemorySize");
        private final Method getFreePhysicalMemorySize = getMethod(mxBean, "getFreePhysicalMemorySize");
        private final Method getFreeSwapSpaceSize = getMethod(mxBean, "getFreeSwapSpaceSize");
        private final Method getProcessCpuTime = getMethod(mxBean, "getProcessCpuTime");
        private final Method getTotalPhysicalMemorySize = getMethod(mxBean, "getTotalPhysicalMemorySize");
        private final Method getTotalSwapSpaceSize = getMethod(mxBean, "getTotalSwapSpaceSize");
        private final Method getMaxFileDescriptorCount = getMethod(mxBean, "getMaxFileDescriptorCount");
        private final Method getOpenFileDescriptorCount = getMethod(mxBean, "getOpenFileDescriptorCount");
        private final Method getProcessCpuLoad = getMethod(mxBean, "getProcessCpuLoad");
        private final Method getSystemCpuLoad = getMethod(mxBean, "getSystemCpuLoad");

        @ProbeName
        String probeName() {
            return "os";
        }

        @Probe
        long committedVirtualMemorySize() throws InvocationTargetException, IllegalAccessException {
            return getCommittedVirtualMemorySize == null ? -1 : (Long) getCommittedVirtualMemorySize.invoke(mxBean);
        }

        @Probe
        long freePhysicalMemorySize() throws InvocationTargetException, IllegalAccessException {
            return getFreePhysicalMemorySize == null ? -1 : (Long) getFreePhysicalMemorySize.invoke(mxBean);
        }

        @Probe
        long freeSwapSpaceSize() throws InvocationTargetException, IllegalAccessException {
            return getFreeSwapSpaceSize == null ? -1 : (Long) getFreeSwapSpaceSize.invoke(mxBean);
        }

        @Probe
        long processCpuTime() throws InvocationTargetException, IllegalAccessException {
            return getProcessCpuTime == null ? -1 : (Long) getProcessCpuTime.invoke(mxBean);
        }

        @Probe
        long totalPhysicalMemorySize() throws InvocationTargetException, IllegalAccessException {
            return getTotalPhysicalMemorySize == null ? -1 : (Long) getTotalPhysicalMemorySize.invoke(mxBean);
        }

        @Probe
        long totalSwapSpaceSize() throws InvocationTargetException, IllegalAccessException {
            return getTotalSwapSpaceSize == null ? -1 : (Long) getTotalSwapSpaceSize.invoke(mxBean);
        }

        @Probe
        long maxFileDescriptorCount() throws InvocationTargetException, IllegalAccessException {
            return getMaxFileDescriptorCount == null ? -1 : (Long) getMaxFileDescriptorCount.invoke(mxBean);
        }

        @Probe
        long openFileDescriptorCount() throws InvocationTargetException, IllegalAccessException {
            return getOpenFileDescriptorCount == null ? -1 : (Long) getOpenFileDescriptorCount.invoke(mxBean);
        }

        @Probe
        double processCpuLoad() throws InvocationTargetException, IllegalAccessException {
            return getProcessCpuLoad == null ? -1 : (Double) getProcessCpuLoad.invoke(mxBean);
        }

        @Probe
        double systemCpuLoad() throws InvocationTargetException, IllegalAccessException {
            return getSystemCpuLoad == null ? -1 : (Double) getSystemCpuLoad.invoke(mxBean);
        }

        @Probe
        double systemLoadAverage() {
            return PERCENTAGE_MULTIPLIER * mxBean.getSystemLoadAverage();
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

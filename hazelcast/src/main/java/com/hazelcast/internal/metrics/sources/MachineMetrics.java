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

package com.hazelcast.internal.metrics.sources;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;

import java.io.File;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;

import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.MetricsSource;

/**
 * A {@link MetricsSource} providing information on runtime, threads,
 * class-loading and OS properties
 */
public final class MachineMetrics implements MetricsSource {

    private static final String[] PROBED_OS_METHODS = { "getCommittedVirtualMemorySize",
            "getFreePhysicalMemorySize", "getFreeSwapSpaceSize", "getProcessCpuTime",
            "getTotalPhysicalMemorySize", "getTotalSwapSpaceSize", "getMaxFileDescriptorCount",
            "getOpenFileDescriptorCount", "getProcessCpuLoad", "getSystemCpuLoad" };

    private final File userHome = new File(System.getProperty("user.home"));
    private final Runtime runtime = Runtime.getRuntime();
    private final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private final ClassLoadingMXBean classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
    private final OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();

    @Override
    public void collectAll(CollectionCycle cycle) {
        cycle.switchContext().namespace("classloading");
        collectProperties(cycle, classLoadingMXBean);
        cycle.switchContext().namespace("os");
        collectProperties(cycle, osMXBean);
        cycle.switchContext().namespace("runtime");
        collectProperties(cycle, runtime);
        collectProperties(cycle, runtimeMXBean);
        cycle.switchContext().namespace("thread");
        collectProperties(cycle, threadMXBean);
        cycle.switchContext().namespace("file.partition").instance("user.home");
        collectProperties(cycle, userHome);
    }

    public static void collectProperties(CollectionCycle cycle, File f) {
        cycle.collect(MANDATORY, "creationTime", f.lastModified());
        cycle.collect(MANDATORY, "freeSpace", f.getFreeSpace());
        cycle.collect(MANDATORY, "totalSpace", f.getTotalSpace());
        cycle.collect(MANDATORY, "usableSpace", f.getUsableSpace());
    }

    public static void collectProperties(CollectionCycle cycle, ClassLoadingMXBean bean) {
        cycle.collect(MANDATORY, "loadedClassCount", bean.getLoadedClassCount());
        cycle.collect(MANDATORY, "totalLoadedClassCount", bean.getTotalLoadedClassCount());
        cycle.collect(MANDATORY, "unloadedClassCount", bean.getUnloadedClassCount());
    }

    public static void collectProperties(CollectionCycle cycle, Runtime runtime) {
        long free = runtime.freeMemory();
        long total = runtime.totalMemory();
        cycle.collect(MANDATORY, "availableProcessors", runtime.availableProcessors());
        cycle.collect(MANDATORY, "freeMemory", free);
        cycle.collect(MANDATORY, "maxMemory", runtime.maxMemory());
        cycle.collect(MANDATORY, "totalMemory", total);
        cycle.collect(MANDATORY, "usedMemory", total - free);
    }

    public static void collectProperties(CollectionCycle cycle, RuntimeMXBean bean) {
        cycle.collect("startTime", bean.getStartTime());
        cycle.collect(MANDATORY, "uptime", bean.getUptime());
    }

    public static void collectProperties(CollectionCycle cycle, ThreadMXBean bean) {
        cycle.collect(MANDATORY, "daemonThreadCount", bean.getDaemonThreadCount());
        cycle.collect(MANDATORY, "peakThreadCount", bean.getPeakThreadCount());
        cycle.collect(MANDATORY, "threadCount", bean.getThreadCount());
        cycle.collect(MANDATORY, "totalStartedThreadCount", bean.getTotalStartedThreadCount());
    }

    public static void collectProperties(CollectionCycle cycle, OperatingSystemMXBean bean) {
        cycle.collect(MANDATORY, bean, PROBED_OS_METHODS);
        cycle.collect(MANDATORY, "systemLoadAverage", bean.getSystemLoadAverage());
    }
}

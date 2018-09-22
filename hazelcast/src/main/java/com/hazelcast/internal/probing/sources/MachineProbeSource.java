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

package com.hazelcast.internal.probing.sources;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;

import java.io.File;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;

import com.hazelcast.internal.probing.ProbeSource;
import com.hazelcast.internal.probing.ProbingCycle;

/**
 * A {@link ProbeSource} providing information on runtime, threads,
 * class-loading and OS properties
 */
public final class MachineProbeSource implements ProbeSource {

    public static final ProbeSource INSTANCE = new MachineProbeSource();

    private static final String[] PROBED_OS_METHODS = { "getCommittedVirtualMemorySize",
            "getFreePhysicalMemorySize", "getFreeSwapSpaceSize", "getProcessCpuTime",
            "getTotalPhysicalMemorySize", "getTotalSwapSpaceSize", "getMaxFileDescriptorCount",
            "getOpenFileDescriptorCount", "getProcessCpuLoad", "getSystemCpuLoad" };

    private final Runtime runtime = Runtime.getRuntime();
    private final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private final File userHome = new File(System.getProperty("user.home"));
    private final ClassLoadingMXBean classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
    private final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

    private MachineProbeSource() {
        // force single instance to avoid multi-registration
    }

    @Override
    public void probeNow(ProbingCycle cycle) {
        cycle.openContext().prefix("classloading");
        probeProperties(cycle, classLoadingMXBean);
        cycle.openContext().prefix("os");
        probeProperties(cycle, operatingSystemMXBean);
        cycle.openContext().prefix("runtime");
        probeProperties(cycle, runtime);
        probeProperties(cycle, runtimeMXBean);
        cycle.openContext().prefix("thread");
        probeProperties(cycle, threadMXBean);
        cycle.openContext().tag(TAG_TYPE, "file.partition").tag(TAG_INSTANCE, "user.home");
        probeProperties(cycle, userHome);
    }

    public static void probeProperties(ProbingCycle cycle, File f) {
        cycle.gather(MANDATORY, "creationTime", f.lastModified());
        cycle.gather(MANDATORY, "freeSpace", f.getFreeSpace());
        cycle.gather(MANDATORY, "totalSpace", f.getTotalSpace());
        cycle.gather(MANDATORY, "usableSpace", f.getUsableSpace());
    }

    public static void probeProperties(ProbingCycle cycle, ClassLoadingMXBean bean) {
        cycle.gather(MANDATORY, "loadedClassesCount", bean.getLoadedClassCount());
        cycle.gather(MANDATORY, "totalLoadedClassesCount", bean.getTotalLoadedClassCount());
        cycle.gather(MANDATORY, "unloadedClassCount", bean.getUnloadedClassCount());
    }

    public static void probeProperties(ProbingCycle cycle, Runtime runtime) {
        long free = runtime.freeMemory();
        long total = runtime.totalMemory();
        cycle.gather(MANDATORY, "availableProcessors", runtime.availableProcessors());
        cycle.gather(MANDATORY, "freeMemory", free);
        cycle.gather(MANDATORY, "maxMemory", runtime.maxMemory());
        cycle.gather(MANDATORY, "totalMemory", total);
        cycle.gather(MANDATORY, "usedMemory", total - free);
    }

    public static void probeProperties(ProbingCycle cycle, RuntimeMXBean bean) {
        cycle.gather(MANDATORY, "uptime", bean.getUptime());
    }

    public static void probeProperties(ProbingCycle cycle, ThreadMXBean bean) {
        cycle.gather(MANDATORY, "daemonThreadCount", bean.getDaemonThreadCount());
        cycle.gather(MANDATORY, "peakThreadCount", bean.getPeakThreadCount());
        cycle.gather(MANDATORY, "threadCount", bean.getThreadCount());
        cycle.gather(MANDATORY, "totalStartedThreadCount", bean.getTotalStartedThreadCount());
    }

    public static void probeProperties(ProbingCycle cycle, OperatingSystemMXBean bean) {
        cycle.gather(MANDATORY, bean, PROBED_OS_METHODS);
        cycle.gather(MANDATORY, "systemLoadAverage", bean.getSystemLoadAverage());
    }
}

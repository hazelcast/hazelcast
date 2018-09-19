package com.hazelcast.internal.probing;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;

import java.io.File;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;

import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;

/**
 * A {@link ProbeSource} providing information on runtime, threads,
 * class-loading and OS properties
 */
final class OsProbeSource implements ProbeSource {

    final Runtime runtime = Runtime.getRuntime();
    final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final File userHome = new File(System.getProperty("user.home"));
    final ClassLoadingMXBean classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
    final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

    @Override
    public void probeIn(ProbingCycle cycle) {
        cycle.openContext().prefix("runtime");
        probeProperties(cycle, runtime);
        probeProperties(cycle, runtimeMXBean);
        cycle.openContext().prefix("thread");
        probeProperties(cycle, threadMXBean);
        cycle.openContext().prefix("classloading");
        probeProperties(cycle, classLoadingMXBean);
        cycle.openContext().prefix("os");
        probeProperties(cycle, operatingSystemMXBean);
        cycle.openContext().tag(TAG_TYPE, "file.partition").tag(TAG_INSTANCE, "user.home");
        probeProperties(cycle, userHome);
    }

    public static void probeProperties(ProbingCycle cycle, File f) {
        cycle.probe("freeSpace", f.getFreeSpace());
        cycle.probe("totalSpace", f.getTotalSpace());
        cycle.probe("usableSpace", f.getUsableSpace());
        cycle.probe("creationTime", f.lastModified());
    }

    public static void probeProperties(ProbingCycle cycle, ClassLoadingMXBean bean) {
        cycle.probe("loadedClassesCount", bean.getLoadedClassCount());
        cycle.probe("totalLoadedClassesCount", bean.getTotalLoadedClassCount());
        cycle.probe("unloadedClassCount", bean.getUnloadedClassCount());
    }

    public static void probeProperties(ProbingCycle cycle, Runtime runtime) {
        long free = runtime.freeMemory();
        long total = runtime.totalMemory();
        cycle.probe("freeMemory", free);
        cycle.probe("totalMemory", total);
        cycle.probe("usedMemory", total - free);
        cycle.probe("maxMemory", runtime.maxMemory());
        cycle.probe("availableProcessors", runtime.availableProcessors());
    }

    public static void probeProperties(ProbingCycle cycle, RuntimeMXBean bean) {
        cycle.probe("uptime", bean.getUptime());
    }

    public static void probeProperties(ProbingCycle cycle, ThreadMXBean bean) {
        cycle.probe("threadCount", bean.getThreadCount());
        cycle.probe("peakThreadCount", bean.getPeakThreadCount());
        cycle.probe("daemonThreadCount", bean.getDaemonThreadCount());
        cycle.probe("totalStartedThreadCount", bean.getTotalStartedThreadCount());
    }

    private static final String[] PROBED_OS_METHODS = { "getCommittedVirtualMemorySize",
            "getFreePhysicalMemorySize", "getFreeSwapSpaceSize", "getProcessCpuTime",
            "getTotalPhysicalMemorySize", "getTotalSwapSpaceSize", "getMaxFileDescriptorCount",
            "getOpenFileDescriptorCount", "getProcessCpuLoad", "getSystemCpuLoad" };

    public static void probeProperties(ProbingCycle cycle, OperatingSystemMXBean bean) {
        cycle.probe(MANDATORY, bean, PROBED_OS_METHODS);
        cycle.probe("systemLoadAverage", bean.getSystemLoadAverage());
    }
}
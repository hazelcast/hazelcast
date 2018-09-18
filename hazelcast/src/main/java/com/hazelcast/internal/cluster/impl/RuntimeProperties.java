package com.hazelcast.internal.cluster.impl;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;

final class RuntimeProperties {

    private int runtime_availableProcessors;
	@Probe(name="date.startTime")
	private long date_startTime;
	@Probe(name="seconds.upTime")
	private long seconds_upTime;
	@Probe(name="memory.maxMemory")
	private long memory_maxMemory; 
	@Probe(name="memory.freeMemory")
	private long memory_freeMemory;
	@Probe(name="memory.totalMemory")
	private long memory_totalMemory;
	@Probe(name="memory.heapMemoryMax")
	private long memory_heapMemoryMax;
	@Probe(name="memory.heapMemoryUsed")
	private long memory_heapMemoryUsed;
	@Probe(name="memory.nonHeapMemoryMax")
	private long memory_nonHeapMemoryMax;
	@Probe(name="memory.nonHeapMemoryUsed")
	private long memory_nonHeapMemoryUsed;
	@Probe(name="runtime.totalLoadedClassCount")
	private long runtime_totalLoadedClassCount;
	@Probe(name="runtime.loadedClassCount")
	private int runtime_loadedClassCount;
	@Probe(name="runtime.unloadedClassCount")
	private long runtime_unloadedClassCount; 
	@Probe(name="runtime.totalStartedThreadCount")
	private long runtime_totalStartedThreadCount;
	@Probe(name="runtime.threadCount")
	private int runtime_threadCount;
	@Probe(name="runtime.peakThreadCount")
	private int runtime_peakThreadCount;
	@Probe(name="runtime.daemonThreadCount")
	private int runtime_daemonThreadCount;
	@Probe(name="osMemory.freePhysicalMemory")
	private long osMemory_freePhysicalMemory;
	@Probe(name="osMemory.committedVirtualMemory")
	private long osMemory_committedVirtualMemory;
	@Probe(name="osMemory.totalPhysicalMemory")
	private long osMemory_totalPhysicalMemory;
	@Probe(name="osSwap.freeSwapSpace")
	private long osSwap_freeSwapSpace;
	@Probe(name="osSwap.totalSwapSpace")
	private long osSwap_totalSwapSpace;
	@Probe(name="os.maxFileDescriptorCount")
	private long os_maxFileDescriptorCount;
	@Probe(name="os.openFileDescriptorCount")
	private long os_openFileDescriptorCount;
	@Probe(name="os.processCpuLoad")
	private long os_processCpuLoad;
	@Probe(name="os.systemLoadAverage")
	private long os_systemLoadAverage;
	@Probe(name="os.systemCpuLoad")
	private long os_systemCpuLoad;
	@Probe(name="os.processCpuTime")
	private long os_processCpuTime;
	@Probe(name="os.availableProcessors")
	private long os_availableProcessors;
	
	public RuntimeProperties() {
		update();
	}
	
	@Probe(name="runtime.availableProcessors")
	public int getRuntimeAvailableProcessors() {
		update();
		return runtime_availableProcessors;
	}
	
	public void update() {
		runtime_availableProcessors = RuntimeAvailableProcessors.get();

		RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
		date_startTime = runtimeMxBean.getStartTime();
		seconds_upTime = runtimeMxBean.getUptime();

		Runtime runtime = Runtime.getRuntime();
		memory_maxMemory = runtime.maxMemory();
		memory_freeMemory = runtime.freeMemory();
		memory_totalMemory = runtime.totalMemory();
		
		MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
		MemoryUsage heapMemory = memoryMxBean.getHeapMemoryUsage();
		memory_heapMemoryMax = heapMemory.getMax();
		memory_heapMemoryUsed = heapMemory.getUsed();

		MemoryUsage nonHeapMemory = memoryMxBean.getNonHeapMemoryUsage();
		memory_nonHeapMemoryMax = nonHeapMemory.getMax();
		memory_nonHeapMemoryUsed = nonHeapMemory.getUsed();
		
        ClassLoadingMXBean clMxBean = ManagementFactory.getClassLoadingMXBean();
		runtime_totalLoadedClassCount = clMxBean.getTotalLoadedClassCount();
		runtime_loadedClassCount = clMxBean.getLoadedClassCount();
		runtime_unloadedClassCount = clMxBean.getUnloadedClassCount();

		ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
		runtime_totalStartedThreadCount = threadMxBean.getTotalStartedThreadCount();
		runtime_threadCount = threadMxBean.getThreadCount();
		runtime_peakThreadCount = threadMxBean.getPeakThreadCount();
		runtime_daemonThreadCount = threadMxBean.getDaemonThreadCount();
		
		OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
		osMemory_freePhysicalMemory = get(osMxBean, "getFreePhysicalMemorySize", -1L);
		osMemory_committedVirtualMemory = get(osMxBean, "getCommittedVirtualMemorySize", -1L);
		osMemory_totalPhysicalMemory = get(osMxBean, "getTotalPhysicalMemorySize", -1L);
		osSwap_freeSwapSpace = get(osMxBean, "getFreeSwapSpaceSize", -1L);
		osSwap_totalSwapSpace = get(osMxBean, "getTotalSwapSpaceSize", -1L);
		os_maxFileDescriptorCount = get(osMxBean, "getMaxFileDescriptorCount", -1L);
		os_openFileDescriptorCount = get(osMxBean, "getOpenFileDescriptorCount", -1L);
		os_processCpuLoad = get(osMxBean, "getProcessCpuLoad", -1L);
		os_systemLoadAverage = get(osMxBean, "getSystemLoadAverage", -1L);
		os_systemCpuLoad = get(osMxBean, "getSystemCpuLoad", -1L);
		os_processCpuTime = get(osMxBean, "getProcessCpuTime", -1L);
		os_availableProcessors = get(osMxBean, "getAvailableProcessors", -1L);
	}
	
	private static final int PERCENT_MULTIPLIER = 100;
	
    private static long get(OperatingSystemMXBean mbean, String methodName, long defaultValue) {
        try {
            Method method = mbean.getClass().getMethod(methodName);
            method.setAccessible(true);
            Object value = method.invoke(mbean);
            if (value instanceof Double) {
                double v = (Double) value;
                return Math.round(v * PERCENT_MULTIPLIER);
            }
            if (value instanceof Number) {
            	return ((Number) value).longValue();
            }
            return defaultValue;
        } catch (RuntimeException e) {
            return defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }
}

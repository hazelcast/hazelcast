package com.hazelcast.management;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to be gather JMX related stats for {@link com.hazelcast.management.DefaultTimedMemberStateFactory}
 */
final class TimedMemberStateFactoryHelper {

    private static final int PERCENT_MULTIPLIER = 100;

    private TimedMemberStateFactoryHelper() { }

    static void registerJMXBeans(HazelcastInstanceImpl instance, MemberStateImpl memberState) {
        final EventService es = instance.node.nodeEngine.getEventService();
        final OperationService os = instance.node.nodeEngine.getOperationService();
        final ConnectionManager cm = instance.node.connectionManager;
        final InternalPartitionService ps = instance.node.partitionService;
        final ProxyService proxyService = instance.node.nodeEngine.getProxyService();
        final ExecutionService executionService = instance.node.nodeEngine.getExecutionService();

        final SerializableMXBeans beans = new SerializableMXBeans();
        final SerializableEventServiceBean esBean = new SerializableEventServiceBean(es);
        beans.setEventServiceBean(esBean);
        final SerializableOperationServiceBean osBean = new SerializableOperationServiceBean(os);
        beans.setOperationServiceBean(osBean);
        final SerializableConnectionManagerBean cmBean = new SerializableConnectionManagerBean(cm);
        beans.setConnectionManagerBean(cmBean);
        final SerializablePartitionServiceBean psBean = new SerializablePartitionServiceBean(ps, instance);
        beans.setPartitionServiceBean(psBean);
        final SerializableProxyServiceBean proxyServiceBean = new SerializableProxyServiceBean(proxyService);
        beans.setProxyServiceBean(proxyServiceBean);

        final ManagedExecutorService systemExecutor = executionService.getExecutor(ExecutionService.SYSTEM_EXECUTOR);
        final ManagedExecutorService asyncExecutor = executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR);
        final ManagedExecutorService scheduledExecutor = executionService.getExecutor(ExecutionService.SCHEDULED_EXECUTOR);
        final ManagedExecutorService clientExecutor = executionService.getExecutor(ExecutionService.CLIENT_EXECUTOR);
        final ManagedExecutorService queryExecutor = executionService.getExecutor(ExecutionService.QUERY_EXECUTOR);
        final ManagedExecutorService ioExecutor = executionService.getExecutor(ExecutionService.IO_EXECUTOR);

        final SerializableManagedExecutorBean systemExecutorBean = new SerializableManagedExecutorBean(systemExecutor);
        final SerializableManagedExecutorBean asyncExecutorBean = new SerializableManagedExecutorBean(asyncExecutor);
        final SerializableManagedExecutorBean scheduledExecutorBean = new SerializableManagedExecutorBean(scheduledExecutor);
        final SerializableManagedExecutorBean clientExecutorBean = new SerializableManagedExecutorBean(clientExecutor);
        final SerializableManagedExecutorBean queryExecutorBean = new SerializableManagedExecutorBean(queryExecutor);
        final SerializableManagedExecutorBean ioExecutorBean = new SerializableManagedExecutorBean(ioExecutor);

        beans.putManagedExecutor(ExecutionService.SYSTEM_EXECUTOR, systemExecutorBean);
        beans.putManagedExecutor(ExecutionService.ASYNC_EXECUTOR, asyncExecutorBean);
        beans.putManagedExecutor(ExecutionService.SCHEDULED_EXECUTOR, scheduledExecutorBean);
        beans.putManagedExecutor(ExecutionService.CLIENT_EXECUTOR, clientExecutorBean);
        beans.putManagedExecutor(ExecutionService.QUERY_EXECUTOR, queryExecutorBean);
        beans.putManagedExecutor(ExecutionService.IO_EXECUTOR, ioExecutorBean);
        memberState.setBeans(beans);
    }

    static void createRuntimeProps(MemberStateImpl memberState) {
        Runtime runtime = Runtime.getRuntime();
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        ClassLoadingMXBean clMxBean = ManagementFactory.getClassLoadingMXBean();
        MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemory = memoryMxBean.getHeapMemoryUsage();
        MemoryUsage nonHeapMemory = memoryMxBean.getNonHeapMemoryUsage();
        Map<String, Long> map = new HashMap<String, Long>();
        map.put("runtime.availableProcessors", Integer.valueOf(runtime.availableProcessors()).longValue());
        map.put("date.startTime", runtimeMxBean.getStartTime());
        map.put("seconds.upTime", runtimeMxBean.getUptime());
        map.put("memory.maxMemory", runtime.maxMemory());
        map.put("memory.freeMemory", runtime.freeMemory());
        map.put("memory.totalMemory", runtime.totalMemory());
        map.put("memory.heapMemoryMax", heapMemory.getMax());
        map.put("memory.heapMemoryUsed", heapMemory.getUsed());
        map.put("memory.nonHeapMemoryMax", nonHeapMemory.getMax());
        map.put("memory.nonHeapMemoryUsed", nonHeapMemory.getUsed());
        map.put("runtime.totalLoadedClassCount", clMxBean.getTotalLoadedClassCount());
        map.put("runtime.loadedClassCount", Integer.valueOf(clMxBean.getLoadedClassCount()).longValue());
        map.put("runtime.unloadedClassCount", clMxBean.getUnloadedClassCount());
        map.put("runtime.totalStartedThreadCount", threadMxBean.getTotalStartedThreadCount());
        map.put("runtime.threadCount", Integer.valueOf(threadMxBean.getThreadCount()).longValue());
        map.put("runtime.peakThreadCount", Integer.valueOf(threadMxBean.getPeakThreadCount()).longValue());
        map.put("runtime.daemonThreadCount", Integer.valueOf(threadMxBean.getDaemonThreadCount()).longValue());

        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        map.put("osMemory.freePhysicalMemory", get(osMxBean, "getFreePhysicalMemorySize", 0L));
        map.put("osMemory.committedVirtualMemory", get(osMxBean, "getCommittedVirtualMemorySize", 0L));
        map.put("osMemory.totalPhysicalMemory", get(osMxBean, "getTotalPhysicalMemorySize", 0L));

        map.put("osSwap.freeSwapSpace", get(osMxBean, "getFreeSwapSpaceSize", 0L));
        map.put("osSwap.totalSwapSpace", get(osMxBean, "getTotalSwapSpaceSize", 0L));
        map.put("os.maxFileDescriptorCount", get(osMxBean, "getMaxFileDescriptorCount", 0L));
        map.put("os.openFileDescriptorCount", get(osMxBean, "getOpenFileDescriptorCount", 0L));
        map.put("os.processCpuLoad", get(osMxBean, "getProcessCpuLoad", -1L));
        map.put("os.systemLoadAverage", get(osMxBean, "getSystemLoadAverage", -1L));
        map.put("os.systemCpuLoad", get(osMxBean, "getSystemCpuLoad", -1L));
        map.put("os.processCpuTime", get(osMxBean, "getProcessCpuTime", 0L));

        map.put("os.availableProcessors", get(osMxBean, "getAvailableProcessors", 0L));

        memberState.setRuntimeProps(map);
    }

    private static Long get(OperatingSystemMXBean mbean, String methodName, Long defaultValue) {
        try {
            Method method = mbean.getClass().getMethod(methodName);
            method.setAccessible(true);
            Object value = method.invoke(mbean);
            if (value instanceof Integer) {
                return (long) (Integer) value;
            }
            if (value instanceof Double) {
                double v = (Double) value;
                return Math.round(v * PERCENT_MULTIPLIER);
            }
            if (value instanceof Long) {
                return (Long) value;
            }
            return defaultValue;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            return defaultValue;
        }
    }

}

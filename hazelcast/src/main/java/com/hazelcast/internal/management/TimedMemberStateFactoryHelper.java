/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.management.dto.ConnectionManagerDTO;
import com.hazelcast.internal.management.dto.EventServiceDTO;
import com.hazelcast.internal.management.dto.MXBeansDTO;
import com.hazelcast.internal.management.dto.ManagedExecutorDTO;
import com.hazelcast.internal.management.dto.OperationServiceDTO;
import com.hazelcast.internal.management.dto.PartitionServiceBeanDTO;
import com.hazelcast.internal.management.dto.ProxyServiceDTO;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.internal.monitor.impl.MemberStateImpl;
import com.hazelcast.internal.nio.NetworkingService;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.internal.util.executor.ManagedExecutorService;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Helper class to be gather JMX related stats for {@link TimedMemberStateFactory}
 */
final class TimedMemberStateFactoryHelper {

    private static final int PERCENT_MULTIPLIER = 100;

    private TimedMemberStateFactoryHelper() { }

    static void registerJMXBeans(HazelcastInstanceImpl instance, MemberStateImpl memberState) {
        final EventService es = instance.node.nodeEngine.getEventService();
        final OperationServiceImpl os = instance.node.nodeEngine.getOperationService();
        final NetworkingService cm = instance.node.networkingService;
        final InternalPartitionService ps = instance.node.partitionService;
        final ProxyService proxyService = instance.node.nodeEngine.getProxyService();
        final ExecutionService executionService = instance.node.nodeEngine.getExecutionService();

        final MXBeansDTO beans = new MXBeansDTO();
        final EventServiceDTO esBean = new EventServiceDTO(es);
        beans.setEventServiceBean(esBean);
        final OperationServiceDTO osBean = new OperationServiceDTO(os);
        beans.setOperationServiceBean(osBean);
        final ConnectionManagerDTO cmBean = new ConnectionManagerDTO(cm);
        beans.setConnectionManagerBean(cmBean);
        final PartitionServiceBeanDTO psBean = new PartitionServiceBeanDTO(ps, instance);
        beans.setPartitionServiceBean(psBean);
        final ProxyServiceDTO proxyServiceBean = new ProxyServiceDTO(proxyService);
        beans.setProxyServiceBean(proxyServiceBean);

        final ManagedExecutorService systemExecutor = executionService.getExecutor(ExecutionService.SYSTEM_EXECUTOR);
        final ManagedExecutorService asyncExecutor = executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR);
        final ManagedExecutorService scheduledExecutor = executionService.getExecutor(ExecutionService.SCHEDULED_EXECUTOR);
        final ManagedExecutorService clientExecutor = executionService.getExecutor(ExecutionService.CLIENT_EXECUTOR);
        final ManagedExecutorService queryExecutor = executionService.getExecutor(ExecutionService.QUERY_EXECUTOR);
        final ManagedExecutorService ioExecutor = executionService.getExecutor(ExecutionService.IO_EXECUTOR);
        final ManagedExecutorService offloadableExecutor = executionService.getExecutor(ExecutionService.OFFLOADABLE_EXECUTOR);

        final ManagedExecutorDTO systemExecutorDTO = new ManagedExecutorDTO(systemExecutor);
        final ManagedExecutorDTO asyncExecutorDTO = new ManagedExecutorDTO(asyncExecutor);
        final ManagedExecutorDTO scheduledExecutorDTO = new ManagedExecutorDTO(scheduledExecutor);
        final ManagedExecutorDTO clientExecutorDTO = new ManagedExecutorDTO(clientExecutor);
        final ManagedExecutorDTO queryExecutorDTO = new ManagedExecutorDTO(queryExecutor);
        final ManagedExecutorDTO ioExecutorDTO = new ManagedExecutorDTO(ioExecutor);
        final ManagedExecutorDTO offloadableExecutorDTO = new ManagedExecutorDTO(offloadableExecutor);

        beans.putManagedExecutor(ExecutionService.SYSTEM_EXECUTOR, systemExecutorDTO);
        beans.putManagedExecutor(ExecutionService.ASYNC_EXECUTOR, asyncExecutorDTO);
        beans.putManagedExecutor(ExecutionService.SCHEDULED_EXECUTOR, scheduledExecutorDTO);
        beans.putManagedExecutor(ExecutionService.CLIENT_EXECUTOR, clientExecutorDTO);
        beans.putManagedExecutor(ExecutionService.QUERY_EXECUTOR, queryExecutorDTO);
        beans.putManagedExecutor(ExecutionService.IO_EXECUTOR, ioExecutorDTO);
        beans.putManagedExecutor(ExecutionService.OFFLOADABLE_EXECUTOR, offloadableExecutorDTO);
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
        final int propertyCount = 29;
        Map<String, Long> map = createHashMap(propertyCount);
        map.put("runtime.availableProcessors", (long) RuntimeAvailableProcessors.get());
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
        map.put("runtime.loadedClassCount", (long) clMxBean.getLoadedClassCount());
        map.put("runtime.unloadedClassCount", clMxBean.getUnloadedClassCount());
        map.put("runtime.totalStartedThreadCount", threadMxBean.getTotalStartedThreadCount());
        map.put("runtime.threadCount", (long) threadMxBean.getThreadCount());
        map.put("runtime.peakThreadCount", (long) threadMxBean.getPeakThreadCount());
        map.put("runtime.daemonThreadCount", (long) threadMxBean.getDaemonThreadCount());

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
        } catch (RuntimeException e) {
            return defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

}

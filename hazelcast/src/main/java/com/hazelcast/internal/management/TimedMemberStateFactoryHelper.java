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

package com.hazelcast.internal.management;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.internal.management.dto.ConnectionManagerDTO;
import com.hazelcast.internal.management.dto.EventServiceDTO;
import com.hazelcast.internal.management.dto.MXBeansDTO;
import com.hazelcast.internal.management.dto.ManagedExecutorDTO;
import com.hazelcast.internal.management.dto.OperationServiceDTO;
import com.hazelcast.internal.management.dto.PartitionServiceBeanDTO;
import com.hazelcast.internal.management.dto.ProxyServiceDTO;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Helper class to be gather JMX related stats for {@link TimedMemberStateFactory}
 */
final class TimedMemberStateFactoryHelper {

    private TimedMemberStateFactoryHelper() { }

    static void registerJMXBeans(HazelcastInstanceImpl instance, MemberStateImpl memberState) {
        final EventService es = instance.node.nodeEngine.getEventService();
        final InternalOperationService os = instance.node.nodeEngine.getOperationService();
        final ConnectionManager cm = instance.node.connectionManager;
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

}

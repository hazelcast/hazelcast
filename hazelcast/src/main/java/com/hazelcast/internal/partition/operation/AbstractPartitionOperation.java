/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.OffloadedReplicationPreparation;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static java.util.Collections.emptySet;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.singleton;

abstract class AbstractPartitionOperation extends Operation implements IdentifiedDataSerializable {

    final Collection<MigrationAwareService> getMigrationAwareServices() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        return nodeEngine.getServices(MigrationAwareService.class);
    }

    final Collection<Operation> createAllReplicationOperations(PartitionReplicationEvent event) {
        return createReplicationOperations(event, false);
    }

    /** non-fragmented service replication operation preparation is always executed on partition operation thread */
    final Collection<Operation> createNonFragmentedReplicationOperations(PartitionReplicationEvent event) {
        if (ThreadUtil.isRunningOnPartitionThread()) {
            return createReplicationOperations(event, true);
        }  else {
            UrgentPartitionRunnable<Collection<Operation>> runnable = new UrgentPartitionRunnable<>(
                    event.getPartitionId(), () -> createReplicationOperations(event, true));
            getNodeEngine().getOperationService().execute(runnable);
            return runnable.future.joinInternal();
        }
    }

    private Collection<Operation> createReplicationOperations(PartitionReplicationEvent event, boolean nonFragmentedOnly) {
        Collection<Operation> operations = new ArrayList<>();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);

        for (ServiceInfo serviceInfo : services) {
            MigrationAwareService service = serviceInfo.getService();
            if (nonFragmentedOnly && service instanceof FragmentedMigrationAwareService) {
                // skip fragmented services
                continue;
            }

            Operation op = service.prepareReplicationOperation(event);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                operations.add(op);
            }
        }
        return operations;
    }

    // must be invoked on partition thread; prepares replication operations within partition thread
    // does not support differential sync
    final Collection<Operation> createFragmentReplicationOperations(PartitionReplicationEvent event, ServiceNamespace ns) {
        assert !(ns instanceof NonFragmentedServiceNamespace) : ns + " should be used only for fragmented services!";
        assertRunningOnPartitionThread();

        Collection<Operation> operations = emptySet();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(FragmentedMigrationAwareService.class);

        for (ServiceInfo serviceInfo : services) {
            FragmentedMigrationAwareService service = serviceInfo.getService();
            if (!service.isKnownServiceNamespace(ns)) {
                continue;
            }

            operations = prepareAndAppendReplicationOperation(event, ns, service, serviceInfo.getName(), operations);
        }
        return operations;
    }

    /** used for offloaded replication op preparation while executing a migration request */
    final Collection<Operation> createFragmentReplicationOperationsOffload(PartitionReplicationEvent event, ServiceNamespace ns,
                                                                           Collection<String> serviceNames) {
        assert !(ns instanceof NonFragmentedServiceNamespace) : ns + " should be used only for fragmented services!";

        Collection<Operation> operations = emptySet();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        for (String serviceName : serviceNames) {
            FragmentedMigrationAwareService service = nodeEngine.getService(serviceName);
            assert service.isKnownServiceNamespace(ns) : ns + " should be known by " + service;

            operations = collectReplicationOperations(event, ns, isRunningOnPartitionThread(), operations, serviceName, service);
        }

        return operations;
    }

    /** used for partition replica sync, supporting offloaded replication op preparation */
    final Collection<Operation> createFragmentReplicationOperationsOffload(PartitionReplicationEvent event, ServiceNamespace ns) {
        assert !(ns instanceof NonFragmentedServiceNamespace) : ns + " should be used only for fragmented services!";

        Collection<Operation> operations = emptySet();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(FragmentedMigrationAwareService.class);

        for (ServiceInfo serviceInfo : services) {
            FragmentedMigrationAwareService service = serviceInfo.getService();
            if (!service.isKnownServiceNamespace(ns)) {
                continue;
            }
            operations = collectReplicationOperations(event, ns, isRunningOnPartitionThread(), operations,
                    serviceInfo.getName(), service);
        }
        return operations;
    }

    /**
     * Collect replication operations of a single fragmented service.
     * If the service implements {@link OffloadedReplicationPreparation} interface, then
     * that service's {@link FragmentedMigrationAwareService#prepareReplicationOperation(PartitionReplicationEvent, Collection)}
     * method will be invoked from the internal {@code ASYNC_EXECUTOR},
     * otherwise it will be invoked from the partition thread.
     */
    @Nullable
    private Collection<Operation> collectReplicationOperations(PartitionReplicationEvent event, ServiceNamespace ns,
                                         boolean runsOnPartitionThread, Collection<Operation> operations,
                                         String serviceName, FragmentedMigrationAwareService service) {
        // execute on current thread if (shouldOffload() ^ runsOnPartitionThread)
        // otherwise explicitly request execution on partition or async executor thread.
        if (runsOnPartitionThread
            ^ (service instanceof OffloadedReplicationPreparation
               && ((OffloadedReplicationPreparation) service).shouldOffload())) {
            operations = prepareAndAppendReplicationOperation(event, ns, service, serviceName, operations);
        } else if (runsOnPartitionThread) {
            // migration aware service requested offload but we are on partition thread
            // execute on async executor
            Future<Operation> future =
                    getNodeEngine().getExecutionService().submit(ExecutionService.ASYNC_EXECUTOR,
                            () -> prepareReplicationOperation(event, ns, service, serviceName));
            try {
                Operation op = future.get();
                operations = appendOperation(operations, op);
            } catch (ExecutionException | CancellationException e) {
                ExceptionUtil.rethrow(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ExceptionUtil.rethrow(e);
            }
        } else {
            // migration aware service did not request offload but execution is not on partition thread
            // must execute replication operation preparation on partition thread
            UrgentPartitionRunnable<Operation> partitionThreadRunnable = new UrgentPartitionRunnable<>(
                    event.getPartitionId(),
                    () -> prepareReplicationOperation(event, ns, service, serviceName));
            getNodeEngine().getOperationService().execute(partitionThreadRunnable);
            Operation op = partitionThreadRunnable.future.joinInternal();
            operations = appendOperation(operations, op);
        }
        return operations;
    }

    private Collection<Operation> appendOperation(Collection<Operation> previous, Operation additional) {
        if (additional == null) {
            return previous;
        }
        if (previous.isEmpty()) {
            previous = singleton(additional);
        } else if (previous.size() == 1) {
            // previous is an immutable singleton list
            previous = newOperationSet(previous);
            previous.add(additional);
        } else {
            previous.add(additional);
        }
        return previous;
    }

    private Operation prepareReplicationOperation(PartitionReplicationEvent event, ServiceNamespace ns,
                    FragmentedMigrationAwareService service, String serviceName) {

        Operation op = service.prepareReplicationOperation(event, singleton(ns));
        if (op == null) {
            return null;
        }

        op.setServiceName(serviceName);
        return op;
    }

    private Collection<Operation> prepareAndAppendReplicationOperation(PartitionReplicationEvent event, ServiceNamespace ns,
            FragmentedMigrationAwareService service, String serviceName, Collection<Operation> operations) {

        Operation op = service.prepareReplicationOperation(event, singleton(ns));
        if (op == null) {
            return operations;
        }

        op.setServiceName(serviceName);

        if (operations.isEmpty()) {
            // generally a namespace belongs to a single service only
            operations = singleton(op);
        } else if (operations.size() == 1) {
            operations = newOperationSet(operations);
            operations.add(op);
        } else {
            operations.add(op);
        }
        return operations;
    }

    @Override
    public final int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    // return a new thread-safe Set populated with all elements from previouis
    Set<Operation> newOperationSet(Collection<Operation> previous) {
        Set<Operation> newSet = newSetFromMap(new ConcurrentHashMap<>());
        newSet.addAll(previous);
        return newSet;
    }
}

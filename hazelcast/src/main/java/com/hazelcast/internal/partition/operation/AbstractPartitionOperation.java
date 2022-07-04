/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.partition.ChunkSupplier;
import com.hazelcast.internal.partition.ChunkedMigrationAwareService;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.OffloadedReplicationPreparation;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static java.util.Collections.emptyList;
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

    /**
     * non-fragmented service replication operation preparation is always executed on partition operation thread
     */
    final Collection<Operation> createNonFragmentedReplicationOperations(PartitionReplicationEvent event) {
        if (ThreadUtil.isRunningOnPartitionThread()) {
            return createReplicationOperations(event, true);
        } else {
            UrgentPartitionRunnable<Collection<Operation>> runnable = new UrgentPartitionRunnable<>(
                    event.getPartitionId(), () -> createReplicationOperations(event, true));
            getNodeEngine().getOperationService().execute(runnable);
            return runnable.future.joinInternal();
        }
    }

    private Collection<Operation> createReplicationOperations(PartitionReplicationEvent event,
                                                              boolean nonFragmentedOnly) {
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

    @Nonnull
    final Collection<ChunkSupplier> collectChunkSuppliers(PartitionReplicationEvent event,
                                                          Collection<String> serviceNames,
                                                          ServiceNamespace namespace) {
        ILogger logger = getLogger();
        logger.fine("Collecting chunk suppliers...");

        Collection<ChunkSupplier> chunkSuppliers = emptyList();

        NodeEngine nodeEngine = getNodeEngine();
        for (String serviceName : serviceNames) {
            Object service = nodeEngine.getService(serviceName);
            if (!(service instanceof ChunkedMigrationAwareService)) {
                // skip not chunked services
                continue;
            }

            chunkSuppliers = collectChunkSuppliers(event, namespace,
                    isRunningOnPartitionThread(), chunkSuppliers, ((ChunkedMigrationAwareService) service));

            if (logger.isFineEnabled()) {
                logger.fine(String.format("Created chunk supplier:[%s, partitionId:%d]",
                        namespace, event.getPartitionId()));
            }
        }

        return chunkSuppliers;
    }

    @Nonnull
    final Collection<ChunkSupplier> collectChunkSuppliers(PartitionReplicationEvent event,
                                                          ServiceNamespace ns) {
        assert !(ns instanceof NonFragmentedServiceNamespace)
                : ns + " should be used only for chunked migrations enabled services!";

        ILogger logger = getLogger();
        logger.fine("Collecting chunk chunk suppliers...");

        Collection<ChunkSupplier> chunkSuppliers = Collections.emptyList();

        NodeEngine nodeEngine = getNodeEngine();
        Collection<ChunkedMigrationAwareService> services
                = nodeEngine.getServices(ChunkedMigrationAwareService.class);

        for (ChunkedMigrationAwareService service : services) {
            if (!service.isKnownServiceNamespace(ns)) {
                continue;
            }

            chunkSuppliers = collectChunkSuppliers(event, ns,
                    isRunningOnPartitionThread(), chunkSuppliers, service);

            if (logger.isFineEnabled()) {
                logger.fine(String.format("Created chunk supplier:[%s, partitionId:%d]",
                        ns, event.getPartitionId()));
            }
        }

        return chunkSuppliers;
    }

    private Collection<ChunkSupplier> collectChunkSuppliers(PartitionReplicationEvent event,
                                                            ServiceNamespace ns,
                                                            boolean currentThreadIsPartitionThread,
                                                            Collection<ChunkSupplier> chunkSuppliers,
                                                            ChunkedMigrationAwareService service) {
        // execute on current thread if (shouldOffload() ^
        // currentThreadIsPartitionThread) otherwise explicitly
        // request execution on partition or async executor thread.
        if (currentThreadIsPartitionThread
                ^ (service instanceof OffloadedReplicationPreparation
                && ((OffloadedReplicationPreparation) service).shouldOffload())) {
            return prepareAndAppendNewChunkSupplier(event, ns, service, chunkSuppliers);
        }

        if (currentThreadIsPartitionThread) {
            ChunkSupplier supplier = service.newChunkSupplier(event, singleton(ns));
            return appendNewElement(chunkSuppliers, supplier);
        }

        // migration aware service did not request offload but
        // execution is not on partition thread must execute
        // chunk preparation on partition thread
        UrgentPartitionRunnable<ChunkSupplier> partitionThreadRunnable = new UrgentPartitionRunnable<>(
                event.getPartitionId(), () -> service.newChunkSupplier(event, singleton(ns)));
        getNodeEngine().getOperationService().execute(partitionThreadRunnable);
        ChunkSupplier supplier = partitionThreadRunnable.future.joinInternal();
        return appendNewElement(chunkSuppliers, supplier);
    }

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

    /**
     * Used for offloaded replication-operation
     * preparation while executing a migration request
     */
    final Collection<Operation> createFragmentReplicationOperationsOffload(PartitionReplicationEvent event,
                                                                           ServiceNamespace ns,
                                                                           Collection<String> serviceNames) {
        assert !(ns instanceof NonFragmentedServiceNamespace) : ns + " should be used only for fragmented services!";

        Collection<Operation> operations = emptySet();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        for (String serviceName : serviceNames) {
            FragmentedMigrationAwareService service = nodeEngine.getService(serviceName);
            assert service.isKnownServiceNamespace(ns) : ns + " should be known by " + service;

            operations = collectReplicationOperations(event, ns,
                    isRunningOnPartitionThread(), operations, serviceName, service);
        }

        return operations;
    }

    /**
     * Used for partition replica sync, supporting
     * offloaded replication-operation preparation
     */
    final Collection<Operation> createFragmentReplicationOperationsOffload(PartitionReplicationEvent event,
                                                                           ServiceNamespace ns) {
        assert !(ns instanceof NonFragmentedServiceNamespace)
                : ns + " should be used only for fragmented services!";

        Collection<Operation> operations = emptySet();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(FragmentedMigrationAwareService.class);

        for (ServiceInfo serviceInfo : services) {
            FragmentedMigrationAwareService service = serviceInfo.getService();
            if (!service.isKnownServiceNamespace(ns)) {
                continue;
            }
            operations = collectReplicationOperations(event, ns,
                    isRunningOnPartitionThread(), operations, serviceInfo.getName(), service);
        }
        return operations;
    }

    /**
     * Collect replication operations of a single fragmented service.
     * <p>
     * If the service implements {@link OffloadedReplicationPreparation}
     * interface, then that service's {@link
     * FragmentedMigrationAwareService#prepareReplicationOperation(PartitionReplicationEvent,
     * Collection)} method will be invoked from the internal {@code ASYNC_EXECUTOR},
     * otherwise it will be invoked from the partition thread.
     */
    @Nullable
    private Collection<Operation> collectReplicationOperations(PartitionReplicationEvent event,
                                                               ServiceNamespace ns,
                                                               boolean currentThreadIsPartitionThread,
                                                               Collection<Operation> operations,
                                                               String serviceName,
                                                               FragmentedMigrationAwareService service) {
        // execute on current thread if (shouldOffload() ^ currentThreadIsPartitionThread)
        // otherwise explicitly request execution on partition or async executor thread.
        if (currentThreadIsPartitionThread
                ^ (service instanceof OffloadedReplicationPreparation
                && ((OffloadedReplicationPreparation) service).shouldOffload())) {
            return prepareAndAppendReplicationOperation(event, ns, service, serviceName, operations);
        }

        if (currentThreadIsPartitionThread) {
            Operation op = prepareReplicationOperation(event, ns, service, serviceName);
            return appendNewElement(operations, op);
        }

        // migration aware service did not request offload but execution is not on partition thread
        // must execute replication operation preparation on partition thread
        UrgentPartitionRunnable<Operation> partitionThreadRunnable = new UrgentPartitionRunnable<>(
                event.getPartitionId(),
                () -> prepareReplicationOperation(event, ns, service, serviceName));
        getNodeEngine().getOperationService().execute(partitionThreadRunnable);
        Operation op = partitionThreadRunnable.future.joinInternal();
        return appendNewElement(operations, op);
    }

    private Collection<ChunkSupplier> prepareAndAppendNewChunkSupplier(PartitionReplicationEvent event,
                                                                       ServiceNamespace ns,
                                                                       ChunkedMigrationAwareService service,
                                                                       Collection<ChunkSupplier> chunkSuppliers) {
        ChunkSupplier supplier = service.newChunkSupplier(event, singleton(ns));

        if (supplier == null) {
            return chunkSuppliers;
        }

        if (isEmpty(chunkSuppliers)) {
            chunkSuppliers = newSetOf(chunkSuppliers);
        }

        chunkSuppliers.add(supplier);

        return chunkSuppliers;
    }

    private <T> Collection<T> appendNewElement(Collection<T> previous, T newElement) {
        if (newElement == null) {
            return previous;
        }

        if (isEmpty(previous)) {
            previous = newSetOf(previous);
        }

        previous.add(newElement);

        return previous;
    }

    private Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                  ServiceNamespace ns,
                                                  FragmentedMigrationAwareService service,
                                                  String serviceName) {
        Operation op = service.prepareReplicationOperation(event, singleton(ns));
        if (op == null) {
            return null;
        }

        op.setServiceName(serviceName);
        return op;
    }

    private Collection<Operation> prepareAndAppendReplicationOperation(PartitionReplicationEvent event,
                                                                       ServiceNamespace ns,
                                                                       FragmentedMigrationAwareService service,
                                                                       String serviceName,
                                                                       Collection<Operation> operations) {
        Operation op = service.prepareReplicationOperation(event, singleton(ns));
        if (op == null) {
            return operations;
        }

        op.setServiceName(serviceName);

        if (isEmpty(operations)) {
            operations = newSetOf(operations);
        }

        operations.add(op);

        return operations;
    }

    @Override
    public final int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    // return a new thread-safe Set populated with all elements from previous
    <T> Set<T> newSetOf(Collection<T> previous) {
        Set<T> newSet = newSetFromMap(new ConcurrentHashMap<>());
        newSet.addAll(previous);
        return newSet;
    }
}

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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.ReplicaFragmentMigrationState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationInterceptor.MigrationParticipant;
import com.hazelcast.internal.partition.impl.MigrationManager;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.logging.Level;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * Sent from the master node to the partition owner. It will perform the migration by preparing the migration operations and
 * sending them to the destination. A response with a value equal to {@link Boolean#TRUE} indicates a successful migration.
 * It runs on the migration source and transfers the partition with multiple shots.
 * It divides the partition data into fragments and send a group of fragments within each shot.
 */
public class MigrationRequestOperation extends BaseMigrationOperation {

    private boolean fragmentedMigrationEnabled;
    private transient ServiceNamespacesContext namespacesContext;

    public MigrationRequestOperation() {
    }

    public MigrationRequestOperation(MigrationInfo migrationInfo, List<MigrationInfo> completedMigrations,
            int partitionStateVersion, boolean fragmentedMigrationEnabled) {
        super(migrationInfo, completedMigrations, partitionStateVersion);
        this.fragmentedMigrationEnabled = fragmentedMigrationEnabled;
    }

    @Override
    public CallStatus call() throws Exception {
        setActiveMigration();
        return new OffloadImpl();
    }

    private final class OffloadImpl extends Offload {
        private OffloadImpl() {
            super(MigrationRequestOperation.this);
        }

        @Override
        public void start() {
            NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            try {
                executeBeforeMigrations();
                namespacesContext = new ServiceNamespacesContext(nodeEngine, getPartitionReplicationEvent());
                invokeMigrationOperation(initialReplicaFragmentMigrationState(), true);
            } catch (Throwable e) {
                logThrowable(e);
                completeMigration(false);
            }
        }
    }

    @Override
    void executeBeforeMigrations() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        PartitionReplica source = migrationInfo.getSource();
        boolean ownerMigration = source != null && source.isIdentical(nodeEngine.getLocalMember());
        if (!ownerMigration) {
            return;
        }

        super.executeBeforeMigrations();
    }

    /**
     * Invokes the {@link MigrationOperation} on the migration destination.
     */
    private void invokeMigrationOperation(ReplicaFragmentMigrationState migrationState, boolean firstFragment) {
        assert ThreadUtil.isRunningOnPartitionThread()
                : "Migration operations must be invoked from a partition thread";
        boolean lastFragment = !namespacesContext.hasNext();
        Operation operation = new MigrationOperation(migrationInfo,
                firstFragment ? completedMigrations : Collections.emptyList(),
                partitionStateVersion, migrationState, firstFragment, lastFragment);

        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            Set<ServiceNamespace> namespaces = migrationState != null
                    ? migrationState.getNamespaceVersionMap().keySet() : emptySet();
            logger.finest("Invoking MigrationOperation for namespaces " + namespaces + " and " + migrationInfo
                    + ", firstFragment: " + firstFragment + ", lastFragment: " + lastFragment);
        }

        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl partitionService = getService();
        ExecutorService asyncExecutor = getNodeEngine().getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);

        Address target = migrationInfo.getDestinationAddress();
        nodeEngine.getOperationService()
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, operation, target)
                .setResultDeserialized(true)
                .setCallTimeout(partitionService.getPartitionMigrationTimeout())
                .invoke()
                .whenCompleteAsync(new MigrationCallback(), asyncExecutor);
    }

    private void trySendNewFragmentV42() {
        try {
            verifyMaster();
            verifyExistingDestination();

            InternalPartitionServiceImpl partitionService = getService();
            MigrationManager migrationManager = partitionService.getMigrationManager();
            MigrationInfo currentActiveMigration = migrationManager.addActiveMigration(migrationInfo);
            if (!migrationInfo.equals(currentActiveMigration)) {
                throw new IllegalStateException("Current active migration " + currentActiveMigration
                        + " is different than expected: " + migrationInfo);
            }

            ReplicaFragmentMigrationState migrationState = createNextReplicaFragmentMigrationState();

            if (migrationState != null) {
                invokeMigrationOperation(migrationState, false);
            } else {
                getLogger().finest("All migration fragments done for " + migrationInfo);
                completeMigration(true);
            }
        } catch (Throwable e) {
            logThrowable(e);
            completeMigration(false);
        }
    }

    private void trySendNewFragment() {
        try {
            verifyMaster();
            verifyExistingDestination();

            InternalPartitionServiceImpl partitionService = getService();
            MigrationManager migrationManager = partitionService.getMigrationManager();
            MigrationInfo currentActiveMigration = migrationManager.addActiveMigration(migrationInfo);
            if (!migrationInfo.equals(currentActiveMigration)) {
                throw new IllegalStateException("Current active migration " + currentActiveMigration
                        + " is different than expected: " + migrationInfo);
            }

            // replication operation preparation may have to happen on partition thread or not
            ReplicaFragmentMigrationState migrationState = createNextReplicaFragmentMigrationState();

            // migration invocation must always happen on partition thread
            if (migrationState != null) {
                // RU_COMPAT_4_2
                if (getNodeEngine().getClusterService().getClusterVersion().isLessThan(Versions.V5_0)) {
                    invokeMigrationOperation(migrationState, false);
                } else {
                    // this is executed in async pool thread
                    // migration ops must be serialized and invoked from partition threads
                    getNodeEngine().getOperationService().execute(new InvokeMigrationOps(migrationState, getPartitionId()));
                }
            } else {
                getLogger().finest("All migration fragments done for " + migrationInfo);
                completeMigration(true);
            }
        } catch (Throwable e) {
            logThrowable(e);
            completeMigration(false);
        }
    }

    private final class InvokeMigrationOps implements PartitionSpecificRunnable, UrgentSystemOperation {

        private final ReplicaFragmentMigrationState migrationState;
        private final int partitionId;

        InvokeMigrationOps(ReplicaFragmentMigrationState migrationState, int partitionId) {
            this.migrationState = migrationState;
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                invokeMigrationOperation(migrationState, false);
            } catch (Throwable t) {
                logThrowable(t);
                completeMigration(false);
            }
        }
    }

    /**
     * Creates an empty {@code ReplicaFragmentMigrationState} to perform a ready-check on destination.
     * That way initial {@code MigrationOperation} will be empty and any failure or retry
     * will be very cheap, instead of copying partition data on each retry.
     */
    private ReplicaFragmentMigrationState initialReplicaFragmentMigrationState() {
        return createReplicaFragmentMigrationState(emptySet(), emptySet());
    }

    private ReplicaFragmentMigrationState createNextReplicaFragmentMigrationState() {
        if (!namespacesContext.hasNext()) {
            return null;
        }

        if (!fragmentedMigrationEnabled) {
            // Drain the iterator completely.
            while (namespacesContext.hasNext()) {
                namespacesContext.next();
            }
            return createAllReplicaFragmentsMigrationState();
        }

        ServiceNamespace namespace = namespacesContext.next();
        if (namespace.equals(NonFragmentedServiceNamespace.INSTANCE)) {
            return createNonFragmentedReplicaFragmentMigrationState();
        }
        return createReplicaFragmentMigrationStateFor(namespace);
    }

    private ReplicaFragmentMigrationState createNonFragmentedReplicaFragmentMigrationState() {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Collection<Operation> operations = createNonFragmentedReplicationOperations(event);
        Collection<ServiceNamespace> namespaces =
                Collections.singleton(NonFragmentedServiceNamespace.INSTANCE);
        return createReplicaFragmentMigrationState(namespaces, operations);
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationStateFor(ServiceNamespace ns) {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Collection<String> serviceNames = namespacesContext.getServiceNames(ns);

        Collection<Operation> operations = createFragmentReplicationOperationsOffload(event, ns, serviceNames);
        return createReplicaFragmentMigrationState(singleton(ns), operations);
    }

    private ReplicaFragmentMigrationState createAllReplicaFragmentsMigrationState() {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Collection<Operation> operations = createAllReplicationOperations(event);
        return createReplicaFragmentMigrationState(namespacesContext.allNamespaces, operations);
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationState(Collection<ServiceNamespace> namespaces,
                                                                              Collection<Operation> operations) {

        InternalPartitionService partitionService = getService();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();
        Map<ServiceNamespace, long[]> versions = new HashMap<>(namespaces.size());
        for (ServiceNamespace namespace : namespaces) {
            long[] v = versionManager.getPartitionReplicaVersions(getPartitionId(), namespace);
            versions.put(namespace, v);
        }

        return new ReplicaFragmentMigrationState(versions, operations);
    }

    @Override
    protected PartitionMigrationEvent getMigrationEvent() {
        return new PartitionMigrationEvent(MigrationEndpoint.SOURCE,
                migrationInfo.getPartitionId(), migrationInfo.getSourceCurrentReplicaIndex(),
                migrationInfo.getSourceNewReplicaIndex(), migrationInfo.getUid());
    }

    @Override
    protected MigrationParticipant getMigrationParticipantType() {
        return MigrationParticipant.SOURCE;
    }

    private PartitionReplicationEvent getPartitionReplicationEvent() {
        return new PartitionReplicationEvent(migrationInfo.getDestinationAddress(),
                migrationInfo.getPartitionId(), migrationInfo.getDestinationNewReplicaIndex());
    }

    private void completeMigration(boolean result) {
        success = result;
        onMigrationComplete();
        sendResponse(result);
    }

    private void logThrowable(Throwable t) {
        Throwable throwableToLog = t;
        if (throwableToLog instanceof ExecutionException) {
            throwableToLog = throwableToLog.getCause() != null ? throwableToLog.getCause() : throwableToLog;
        }
        Level level = getLogLevel(throwableToLog);
        getLogger().log(level, "Failure while executing " + migrationInfo, throwableToLog);
    }

    private Level getLogLevel(Throwable e) {
        return (e instanceof MemberLeftException || e instanceof InterruptedException)
                || !getNodeEngine().isRunning() ? Level.INFO : Level.WARNING;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.MIGRATION_REQUEST;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(fragmentedMigrationEnabled);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        fragmentedMigrationEnabled = in.readBoolean();
    }

    /**
     * Processes the migration result sent from the migration destination and sends the response to the caller of this operation.
     * A response equal to {@link Boolean#TRUE} indicates successful migration.
     */
    private final class MigrationCallback implements BiConsumer<Object, Throwable> {

        private MigrationCallback() {
        }

        @Override
        public void accept(Object result, Throwable throwable) {
            if (throwable != null) {
                logThrowable(throwable);
                completeMigration(false);
            } else if (Boolean.TRUE.equals(result)) {
                // RU_COMPAT_4_2
                if (getNodeEngine().getClusterService().getClusterVersion().isGreaterOrEqual(Versions.V5_0)) {
                    // ASYNC executor is of CONCRETE type (does not share threads with other executors)
                    // and is never used for user-supplied code.
                    getNodeEngine().getExecutionService().submit(ExecutionService.ASYNC_EXECUTOR,
                            () -> trySendNewFragment());
                } else {
                    OperationService operationService = getNodeEngine().getOperationService();
                    operationService.execute(new SendNewMigrationFragmentRunnableV42());
                }
            } else {
                ILogger logger = getLogger();
                if (logger.isFineEnabled()) {
                    logger.fine("Received false response from migration destination -> " + migrationInfo);
                }
                completeMigration(false);
            }
        }
    }

    // RU_COMPAT_4_2
    private final class SendNewMigrationFragmentRunnableV42 implements PartitionSpecificRunnable, UrgentSystemOperation {

        @Override
        public int getPartitionId() {
            return MigrationRequestOperation.this.getPartitionId();
        }

        @Override
        public void run() {
            trySendNewFragmentV42();
        }

    }

    private static class ServiceNamespacesContext {
        final Collection<ServiceNamespace> allNamespaces = new HashSet<>();
        final Map<ServiceNamespace, Collection<String>> namespaceToServices = new HashMap<>();

        final Iterator<ServiceNamespace> namespaceIterator;

        ServiceNamespacesContext(NodeEngineImpl nodeEngine, PartitionReplicationEvent event) {
            Collection<ServiceInfo> services = nodeEngine.getServiceInfos(FragmentedMigrationAwareService.class);
            for (ServiceInfo serviceInfo : services) {
                FragmentedMigrationAwareService service = serviceInfo.getService();
                Collection<ServiceNamespace> namespaces = service.getAllServiceNamespaces(event);
                if (namespaces != null) {
                    String serviceName = serviceInfo.getName();
                    allNamespaces.addAll(namespaces);

                    addNamespaceToServiceMappings(namespaces, serviceName);
                }
            }

            allNamespaces.add(NonFragmentedServiceNamespace.INSTANCE);
            namespaceIterator = allNamespaces.iterator();
        }

        private void addNamespaceToServiceMappings(Collection<ServiceNamespace> namespaces, String serviceName) {
            for (ServiceNamespace ns : namespaces) {
                Collection<String> serviceNames = namespaceToServices.get(ns);
                if (serviceNames == null) {
                    // generally a namespace belongs to a single service only
                    namespaceToServices.put(ns, singleton(serviceName));
                } else if (serviceNames.size() == 1) {
                    serviceNames = new HashSet<>(serviceNames);
                    serviceNames.add(serviceName);
                    namespaceToServices.put(ns, serviceNames);
                } else {
                    serviceNames.add(serviceName);
                }
            }
        }

        boolean hasNext() {
            return namespaceIterator.hasNext();
        }

        ServiceNamespace next() {
            return namespaceIterator.next();
        }

        Collection<String> getServiceNames(ServiceNamespace ns) {
            return namespaceToServices.get(ns);
        }
    }
}

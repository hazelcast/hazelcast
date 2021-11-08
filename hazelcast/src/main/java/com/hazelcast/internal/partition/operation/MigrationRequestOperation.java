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
import com.hazelcast.internal.partition.ChunkSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import javax.annotation.Nullable;
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

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.CollectionUtil.isNotEmpty;
import static java.util.Collections.emptyList;
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
    private transient Map<ServiceNamespace, Collection<ChunkSupplier>>
            namespaceToSuppliers = new HashMap<>();

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
                // migration ops must be serialized and invoked from partition threads
                getNodeEngine().getOperationService().execute(new InvokeMigrationOps(migrationState, getPartitionId()));
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
        return createReplicaFragmentMigrationState(emptySet(), emptySet(), emptyList());
    }

    private ReplicaFragmentMigrationState createNextReplicaFragmentMigrationState() {
        ReplicaFragmentMigrationState nextChunkedState = createNextChunkedState();
        if (nextChunkedState != null) {
            return nextChunkedState;
        }

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

        Collection<ChunkSupplier> chunkSuppliers = createChunkSuppliersOf(namespace);
        if (isNotEmpty(chunkSuppliers)) {
            return createChunkedReplicaState(namespace, chunkSuppliers);
        }

        return createReplicaFragmentMigrationStateFor(namespace);
    }

    // TODO add other services data with same namespace
    private Collection<ChunkSupplier> createChunkSuppliersOf(ServiceNamespace namespace) {
        return namespaceToSuppliers.computeIfAbsent(namespace,
                ns -> {
                    Collection<String> serviceNames = namespacesContext.getServiceNames(ns);
                    return collectChunkSuppliers(getPartitionReplicationEvent(), serviceNames, ns);
                });
    }

    @Nullable
    private ReplicaFragmentMigrationState createNextChunkedState() {
        // if no current namespace exists, this is
        // the start of migration process, just return
        ServiceNamespace currentNS = namespacesContext.current();
        if (currentNS == null) {
            return null;
        }

        Collection<ChunkSupplier> chunkSuppliers = namespaceToSuppliers.get(currentNS);
        if (chunkSuppliers == null) {
            // this namespace does not support chunked
            // migration, hence no chunk supplier.
            return null;
        }

        // remove finished suppliers
        Iterator<ChunkSupplier> iterator = chunkSuppliers.iterator();
        while (iterator.hasNext()) {
            ChunkSupplier chunkSupplier = iterator.next();
            if (!chunkSupplier.hasNext()) {
                iterator.remove();
            }
        }

        if (isEmpty(chunkSuppliers)) {
            namespaceToSuppliers.remove(currentNS);
            return null;
        }

        // we still have unfinished suppliers
        return createReplicaFragmentMigrationState(singleton(currentNS),
                emptyList(), chunkSuppliers);
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationStateFor(ServiceNamespace ns) {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Collection<String> serviceNames = namespacesContext.getServiceNames(ns);
        Collection<Operation> operations = createFragmentReplicationOperationsOffload(event, ns, serviceNames);
        return createReplicaFragmentMigrationState(singleton(ns), operations, emptyList());
    }

    private ReplicaFragmentMigrationState createNonFragmentedReplicaFragmentMigrationState() {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Collection<Operation> operations = createNonFragmentedReplicationOperations(event);
        Collection<ServiceNamespace> namespaces =
                Collections.singleton(NonFragmentedServiceNamespace.INSTANCE);
        return createReplicaFragmentMigrationState(namespaces, operations, emptyList());
    }

    private ReplicaFragmentMigrationState createChunkedReplicaState(ServiceNamespace ns,
                                                                    Collection<ChunkSupplier> suppliers) {
        return createReplicaFragmentMigrationState(singleton(ns), emptyList(), suppliers);
    }

    private ReplicaFragmentMigrationState createAllReplicaFragmentsMigrationState() {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Collection<Operation> operations = createAllReplicationOperations(event);
        return createReplicaFragmentMigrationState(namespacesContext.allNamespaces, operations, emptyList());
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationState(Collection<ServiceNamespace> namespaces,
                                                                              Collection<Operation> operations,
                                                                              Collection<ChunkSupplier> suppliers) {
        InternalPartitionService partitionService = getService();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();
        Map<ServiceNamespace, long[]> versions = new HashMap<>(namespaces.size());
        for (ServiceNamespace namespace : namespaces) {
            long[] v = versionManager.getPartitionReplicaVersions(getPartitionId(), namespace);
            versions.put(namespace, v);
        }

        return new ReplicaFragmentMigrationState(versions, operations, suppliers);
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
                // ASYNC executor is of CONCRETE type (does not share threads with other executors)
                // and is never used for user-supplied code.
                getNodeEngine().getExecutionService().submit(ExecutionService.ASYNC_EXECUTOR,
                        () -> trySendNewFragment());
            } else {
                ILogger logger = getLogger();
                if (logger.isFineEnabled()) {
                    logger.fine("Received false response from migration destination -> " + migrationInfo);
                }
                completeMigration(false);
            }
        }
    }

    /**
     * A namespace is used to group single or multiple services'.
     * <p>
     * For instance, map-service uses same namespace with lock-service
     * and ring-buffer-service when it needs to use their functionality.
     *
     * <p>
     * To clarify the concept, this is a sketch of multiple
     * services which are sharing the same namespace
     *
     * <pre>
     *
     * serviceName-1    serviceName-2    serviceName-3
     * /---------\      /---------\      /---------\
     * |         |      |         |      |         |
     * |         |      |         |      |         |
     * |         |      |         |      |         |
     * /-------------------------------------------\
     * | A SHARED NAMESPACE OF MULTIPLE SERVICES   |
     * \-------------------------------------------/
     * |         |      |         |      |         |
     * |         |      |         |      |         |
     * |         |      |         |      |         |
     * \---------/      \---------/      \---------/
     * </pre>
     */
    private final class ServiceNamespacesContext {

        private final Iterator<ServiceNamespace> namespaceIterator;
        private final Set<ServiceNamespace> allNamespaces = new HashSet<>();
        private final Map<ServiceNamespace, Collection<String>> namespaceToServices = new HashMap<>();

        private ServiceNamespace currentNamespace;

        private ServiceNamespacesContext(NodeEngineImpl nodeEngine, PartitionReplicationEvent event) {
            nodeEngine.forEachMatchingService(FragmentedMigrationAwareService.class, serviceInfo -> {
                // get all namespaces of a service
                Collection<ServiceNamespace> namespaces = getAllServiceNamespaces(serviceInfo, event);
                if (isNotEmpty(namespaces)) {
                    // update collection of unique namespaces
                    allNamespaces.addAll(namespaces);
                    // map namespace to serviceName
                    namespaces.forEach(ns -> mapNamespaceToService(ns, serviceInfo.getName()));
                }
            });

            // add a namespace to represent non-fragmented services
            allNamespaces.add(NonFragmentedServiceNamespace.INSTANCE);

            namespaceIterator = allNamespaces.iterator();
        }

        private void mapNamespaceToService(ServiceNamespace ns, String serviceName) {
            Collection<String> existingServiceNames = namespaceToServices.get(ns);
            if (existingServiceNames == null) {
                // generally a namespace belongs to a single service only
                namespaceToServices.put(ns, singleton(serviceName));
                return;
            }

            if (existingServiceNames.size() == 1) {
                existingServiceNames = new HashSet<>(existingServiceNames);
                namespaceToServices.put(ns, existingServiceNames);
            }

            existingServiceNames.add(serviceName);

        }

        private Collection<ServiceNamespace> getAllServiceNamespaces(ServiceInfo serviceInfo,
                                                                     PartitionReplicationEvent event) {
            return ((FragmentedMigrationAwareService) serviceInfo
                    .getService()).getAllServiceNamespaces(event);
        }

        private boolean hasNext() {
            return namespaceIterator.hasNext();
        }

        private ServiceNamespace current() {
            return currentNamespace;
        }

        private ServiceNamespace next() {
            currentNamespace = namespaceIterator.next();
            return currentNamespace;
        }

        private Collection<String> getServiceNames(ServiceNamespace ns) {
            return namespaceToServices.get(ns);
        }
    }
}

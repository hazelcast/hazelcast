/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.InternalReplicaFragmentNamespace;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.ReplicaFragmentMigrationState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationManager;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ReplicaFragmentNamespace;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.SimpleExecutionCallback;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.internal.cluster.Versions.V3_9;
import static com.hazelcast.internal.partition.ReplicaFragmentMigrationState.newDefaultReplicaFragmentMigrationState;
import static com.hazelcast.internal.partition.ReplicaFragmentMigrationState.newGroupedReplicaFragmentMigrationState;
import static java.util.Collections.singletonList;

/**
 * Migration request operation used by Hazelcast version 3.9
 * Sent from the master node to the partition owner. It will perform the migration by preparing the migration operations and
 * sending them to the destination. A response with a value equal to {@link Boolean#TRUE} indicates a successful migration.
 * It runs on the migration source and transfers the partition with multiple shots.
 * It divides the partition data into fragments and send a group of fragments within each shot.
 */
public class MigrationRequestOperation extends BaseMigrationSourceOperation {

    private FragmentedMigrationContext fragmentedMigrationContext;

    public MigrationRequestOperation() {
    }

    public MigrationRequestOperation(MigrationInfo migrationInfo, int partitionStateVersion) {
        super(migrationInfo, partitionStateVersion);
    }

    @Override
    public void run() {
        verifyMasterOnMigrationSource();
        NodeEngine nodeEngine = getNodeEngine();

        Address source = migrationInfo.getSource();
        Address destination = migrationInfo.getDestination();
        verifyExistingTarget(nodeEngine, destination);

        if (destination.equals(source)) {
            getLogger().warning("Source and destination addresses are the same! => " + toString());
            setFailed();
            return;
        }

        InternalPartition partition = getPartition();
        verifySource(nodeEngine.getThisAddress(), partition);

        setActiveMigration();

        if (!migrationInfo.startProcessing()) {
            getLogger().warning("Migration is cancelled -> " + migrationInfo);
            setFailed();
            return;
        }

        try {
            executeBeforeMigrations();
            initializeFragmentedMigrationContext();
            ReplicaFragmentMigrationState migrationState = createReplicaFragmentMigrationState();
            invokeMigrationOperation(destination, migrationState, true);
            returnResponse = false;
        } catch (Throwable e) {
            logThrowable(e);
            setFailed();
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    /**
     * Invokes the {@link MigrationOperation} on the migration destination.
     */
    private void invokeMigrationOperation(Address destination, ReplicaFragmentMigrationState migrationState,
                                          boolean firstFragment)
            throws IOException {
        boolean lastFragment = fragmentedMigrationContext.isCompleted();
        Operation operation = new MigrationOperation(migrationInfo, partitionStateVersion, migrationState,
                                                     firstFragment, lastFragment);

        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Invoking MigrationOperation for namespaces: " + migrationState.getNamespaces().keySet()
                    + " lastFragment: " + lastFragment);
        }

        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl partitionService = getService();

        nodeEngine.getOperationService()
                  .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, operation, destination)
                  .setExecutionCallback(new MigrationCallback())
                  .setResultDeserialized(true)
                  .setCallTimeout(partitionService.getPartitionMigrationTimeout())
                  .setTryCount(InternalPartitionService.MIGRATION_RETRY_COUNT)
                  .setTryPauseMillis(InternalPartitionService.MIGRATION_RETRY_PAUSE)
                  .setReplicaIndex(getReplicaIndex())
                  .invoke();
    }

    private void trySendNewFragment() {
        try {
            verifyMasterOnMigrationSource();
            NodeEngine nodeEngine = getNodeEngine();

            Address destination = migrationInfo.getDestination();
            verifyExistingTarget(nodeEngine, destination);

            InternalPartitionServiceImpl partitionService = getService();
            MigrationManager migrationManager = partitionService.getMigrationManager();
            MigrationInfo currentActiveMigration = migrationManager.setActiveMigration(migrationInfo);
            if (!migrationInfo.equals(currentActiveMigration)) {
                throw new IllegalStateException("Current active migration " + currentActiveMigration
                        + " is different than expected: " + migrationInfo);
            }

            ReplicaFragmentMigrationState migrationState = createReplicaFragmentMigrationState();
            if (migrationState != null) {
                invokeMigrationOperation(destination, migrationState, false);
            } else {
                getLogger().finest("All fragments done.");
                completeMigration(true);
            }
        } catch (Throwable e) {
            logThrowable(e);
            completeMigration(false);
        }
    }

    private void initializeFragmentedMigrationContext() {
        Map<String, Collection<ReplicaFragmentNamespace>> fragmentNamespaces = getAllReplicaFragmentNamespaces();
        fragmentedMigrationContext = new FragmentedMigrationContext(fragmentNamespaces);
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationState() {
        Collection<ReplicaFragmentNamespace> namespaces = fragmentedMigrationContext.getReplicaFragmentNamespacesToMigrate();
        if (namespaces == null) {
            return null;
        }

        if (namespaces.size() == 1 && namespaces.iterator().next().equals(InternalReplicaFragmentNamespace.INSTANCE)) {
            return createDefaultReplicaFragmentMigrationState();
        }

        return createGroupedReplicaFragmentMigrationState(namespaces);
    }

    private ReplicaFragmentMigrationState createDefaultReplicaFragmentMigrationState() {
        Collection<Operation> operations = new ArrayList<Operation>();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);
        PartitionReplicationEvent replicationEvent = getPartitionReplicationEvent();

        for (ServiceInfo serviceInfo : services) {
            MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();
            if (service instanceof FragmentedMigrationAwareService) {
                continue;
            }

            Operation op = service.prepareReplicationOperation(replicationEvent);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                operations.add(op);
            }
        }

        int partitionId = getPartitionId();
        InternalPartitionService partitionService = getService();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();
        long[] versions = versionManager.getPartitionReplicaVersions(partitionId, InternalReplicaFragmentNamespace.INSTANCE);

        return newDefaultReplicaFragmentMigrationState(versions, operations);
    }

    private ReplicaFragmentMigrationState createGroupedReplicaFragmentMigrationState(Collection<ReplicaFragmentNamespace>
                                                                                     namespaces) {
        String serviceName = namespaces.iterator().next().getServiceName();

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        FragmentedMigrationAwareService service = nodeEngine.getService(serviceName);
        PartitionReplicationEvent replicationEvent = getPartitionReplicationEvent();

        Operation op = service.prepareReplicationOperation(replicationEvent, namespaces);
        if (op != null) {
            op.setServiceName(serviceName);

            InternalPartitionService partitionService = getService();
            PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();
            Map<ReplicaFragmentNamespace, long[]> versions = new HashMap<ReplicaFragmentNamespace, long[]>();
            for (ReplicaFragmentNamespace namespace : namespaces) {
                long[] v = versionManager.getPartitionReplicaVersions(getPartitionId(), namespace);
                versions.put(namespace, v);
            }

            return newGroupedReplicaFragmentMigrationState(versions, op);
        }

        return null;
    }

    private Map<String, Collection<ReplicaFragmentNamespace>> getAllReplicaFragmentNamespaces() {
        Map<String, Collection<ReplicaFragmentNamespace>> namespaces
                = new HashMap<String, Collection<ReplicaFragmentNamespace>>();

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(FragmentedMigrationAwareService.class);
        PartitionReplicationEvent replicationEvent = getPartitionReplicationEvent();

        for (ServiceInfo serviceInfo : services) {
            FragmentedMigrationAwareService service = (FragmentedMigrationAwareService) serviceInfo.getService();
            Collection<ReplicaFragmentNamespace> serviceNamespaces = service.getAllFragmentNamespaces(replicationEvent);
            namespaces.put(serviceInfo.getName(), serviceNamespaces);
        }

        ReplicaFragmentNamespace defaultNamespace = InternalReplicaFragmentNamespace.INSTANCE;
        namespaces.put(defaultNamespace.getServiceName(), singletonList(defaultNamespace));

        ILogger logger = getLogger();
        for (Entry<String, Collection<ReplicaFragmentNamespace>> e : namespaces.entrySet()) {
            if (logger.isFinestEnabled()) {
                logger.finest("MIGRATION FRAGMENTS -> service: " + e.getKey() + " namespaces: " + e.getValue());
            }
        }

        return namespaces;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.MIGRATION_REQUEST;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
    }

    /**
     * Processes the migration result sent from the migration destination and sends the response to the caller of this operation.
     * A response equal to {@link Boolean#TRUE} indicates successful migration.
     */
    private final class MigrationCallback extends SimpleExecutionCallback<Object> {

        private MigrationCallback() {
        }

        @Override
        public void notify(Object result) {
            if (Boolean.TRUE.equals(result)) {
                InternalOperationService operationService = (InternalOperationService) getNodeEngine().getOperationService();
                operationService.execute(new SendNewMigrationFragmentRunnable());
            } else {
                completeMigration(false);
            }
        }
    }

    private final class SendNewMigrationFragmentRunnable implements PartitionSpecificRunnable {

        @Override
        public int getPartitionId() {
            return MigrationRequestOperation.this.getPartitionId();
        }

        @Override
        public void run() {
            trySendNewFragment();
        }

    }

    private static final class FragmentedMigrationContext {

        private Map<String, Collection<ReplicaFragmentNamespace>> replicaFragmentNamespaces;

        private Map<ReplicaFragmentNamespace, Boolean> replicaFragmentStatuses = new HashMap<ReplicaFragmentNamespace, Boolean>();

        FragmentedMigrationContext(Map<String, Collection<ReplicaFragmentNamespace>> replicaFragmentNamespaces) {
            this.replicaFragmentNamespaces = replicaFragmentNamespaces;
            for (Collection<ReplicaFragmentNamespace> serviceNamespaces : replicaFragmentNamespaces.values()) {
                for (ReplicaFragmentNamespace namespace : serviceNamespaces) {
                    replicaFragmentStatuses.put(namespace, false);
                }
            }
        }

        Collection<ReplicaFragmentNamespace> getReplicaFragmentNamespacesToMigrate() {
            if (!replicaFragmentStatuses.get(InternalReplicaFragmentNamespace.INSTANCE)) {
                replicaFragmentStatuses.put(InternalReplicaFragmentNamespace.INSTANCE, true);
                return singletonList(InternalReplicaFragmentNamespace.INSTANCE);
            }

            for (Collection<ReplicaFragmentNamespace> namespaces : replicaFragmentNamespaces.values()) {
                Collection<ReplicaFragmentNamespace> notMigrated = new ArrayList<ReplicaFragmentNamespace>();
                for (ReplicaFragmentNamespace namespace : namespaces) {
                    if (!replicaFragmentStatuses.get(namespace)) {
                        notMigrated.add(namespace);
                    }
                }
                if (notMigrated.size() > 0) {
                    ReplicaFragmentNamespace namespace = notMigrated.iterator().next();
                    replicaFragmentStatuses.put(namespace, true);
                    return  singletonList(namespace);
                }
            }

            return null;
        }

        boolean isCompleted() {
            for (Entry<ReplicaFragmentNamespace, Boolean> e : replicaFragmentStatuses.entrySet()) {
                if (e.getValue()) {
                    continue;
                }

                return false;
            }

            return true;
        }
    }

}

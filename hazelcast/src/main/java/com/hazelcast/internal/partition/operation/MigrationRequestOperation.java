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
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ReplicaFragmentNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.SimpleExecutionCallback;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Migration request operation used by Hazelcast version 3.9
 * Sent from the master node to the partition owner. It will perform the migration by preparing the migration operations and
 * sending them to the destination. A response with a value equal to {@link Boolean#TRUE} indicates a successful migration.
 * It runs on the migration source and transfers the partition with multiple shots.
 * It divides the partition data into fragments and send a group of fragments within each shot.
 */
public class MigrationRequestOperation extends BaseMigrationSourceOperation {

    private boolean fragmentedMigrationEnabled;
    private transient Iterator<ReplicaFragmentNamespace> namespaceIterator;

    public MigrationRequestOperation() {
    }

    public MigrationRequestOperation(MigrationInfo migrationInfo, int partitionStateVersion,
            boolean fragmentedMigrationEnabled) {
        super(migrationInfo, partitionStateVersion);
        this.fragmentedMigrationEnabled = fragmentedMigrationEnabled;
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
                                          boolean firstFragment) throws IOException {

        boolean lastFragment = !fragmentedMigrationEnabled || !namespaceIterator.hasNext();
        Operation operation = new MigrationOperation(migrationInfo, partitionStateVersion, migrationState,
                                                     firstFragment, lastFragment);

        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            Set<ReplicaFragmentNamespace> namespaces = migrationState != null
                    ? migrationState.getNamespaceVersionMap().keySet() : Collections.<ReplicaFragmentNamespace>emptySet();
            logger.finest("Invoking MigrationOperation for namespaces " + namespaces + " and " + migrationInfo
                    + ", lastFragment: " + lastFragment);
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
            assert fragmentedMigrationEnabled : "Fragmented migration should be enabled!";
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
                getLogger().finest("All migration fragments done for " + migrationInfo);
                completeMigration(true);
            }
        } catch (Throwable e) {
            logThrowable(e);
            completeMigration(false);
        }
    }

    private void initializeFragmentedMigrationContext() {
        if (!fragmentedMigrationEnabled) {
            return;
        }
        namespaceIterator = getAllReplicaFragmentNamespaces().iterator();
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationState() {
        if (fragmentedMigrationEnabled) {
            if (!namespaceIterator.hasNext()) {
                 return null;
            }
            ReplicaFragmentNamespace namespace = namespaceIterator.next();
            if (namespace.equals(InternalReplicaFragmentNamespace.INSTANCE)) {
                return createInternalReplicaFragmentMigrationState();
            }
            return createReplicaFragmentMigrationStateFor(namespace);
        } else {
            return createAllReplicaFragmentsMigrationState();
        }
    }

    private ReplicaFragmentMigrationState createInternalReplicaFragmentMigrationState() {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Collection<Operation> operations = createNonFragmentedReplicationOperations(event);
        Collection<ReplicaFragmentNamespace> namespaces =
                Collections.<ReplicaFragmentNamespace>singleton(InternalReplicaFragmentNamespace.INSTANCE);
        return createReplicaFragmentMigrationState(namespaces, operations);
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationStateFor(ReplicaFragmentNamespace ns) {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Operation operation = createFragmentReplicationOperation(event, ns);
        return createReplicaFragmentMigrationState(singleton(ns), singleton(operation));
    }

    private ReplicaFragmentMigrationState createAllReplicaFragmentsMigrationState() {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Collection<Operation> operations = createAllReplicationOperations(event);
        return createReplicaFragmentMigrationState(getAllReplicaFragmentNamespaces(), operations);
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationState(Collection<ReplicaFragmentNamespace>
            namespaces, Collection<Operation> operations) {

        InternalPartitionService partitionService = getService();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();
        Map<ReplicaFragmentNamespace, long[]> versions = new HashMap<ReplicaFragmentNamespace, long[]>(namespaces.size());
        for (ReplicaFragmentNamespace namespace : namespaces) {
            long[] v = versionManager.getPartitionReplicaVersions(getPartitionId(), namespace);
            versions.put(namespace, v);
        }

        return new ReplicaFragmentMigrationState(versions, operations);
    }

    private Collection<ReplicaFragmentNamespace> getAllReplicaFragmentNamespaces() {
        Collection<ReplicaFragmentNamespace> namespaces = new HashSet<ReplicaFragmentNamespace>();

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(FragmentedMigrationAwareService.class);
        PartitionReplicationEvent replicationEvent = getPartitionReplicationEvent();

        for (ServiceInfo serviceInfo : services) {
            FragmentedMigrationAwareService service = serviceInfo.getService();
            Collection<ReplicaFragmentNamespace> serviceNamespaces = service.getAllFragmentNamespaces(replicationEvent);
            namespaces.addAll(serviceNamespaces);
        }
        namespaces.add(InternalReplicaFragmentNamespace.INSTANCE);

        return namespaces;
    }

    @Override
    public int getId() {
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
    private final class MigrationCallback extends SimpleExecutionCallback<Object> {

        private MigrationCallback() {
        }

        @Override
        public void notify(Object result) {
            if (Boolean.TRUE.equals(result)) {
                if (fragmentedMigrationEnabled) {
                    InternalOperationService operationService = (InternalOperationService) getNodeEngine().getOperationService();
                    operationService.execute(new SendNewMigrationFragmentRunnable());
                } else {
                    completeMigration(true);
                }
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
}

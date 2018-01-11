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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
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
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.SimpleExecutionCallback;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
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
    private transient ServiceNamespacesContext namespacesContext;

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
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();

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
            namespacesContext = new ServiceNamespacesContext(nodeEngine, getPartitionReplicationEvent());
            ReplicaFragmentMigrationState migrationState = fragmentedMigrationEnabled
                    ? createNextReplicaFragmentMigrationState()
                    : createAllReplicaFragmentsMigrationState();
            invokeMigrationOperation(destination, migrationState, true);
            returnResponse = false;
        } catch (Throwable e) {
            logThrowable(e);
            setFailed();
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    @Override
    void onMigrationStart() {
        ((OperationServiceImpl) getNodeEngine().getOperationService()).onStartAsyncOperation(this);
        super.onMigrationStart();
    }

    @Override
    void onMigrationComplete(boolean result) {
        try {
            super.onMigrationComplete(result);
        } finally {
            ((OperationServiceImpl) getNodeEngine().getOperationService()).onCompletionAsyncOperation(this);
        }
    }

    /**
     * Invokes the {@link MigrationOperation} on the migration destination.
     */
    private void invokeMigrationOperation(Address destination, ReplicaFragmentMigrationState migrationState,
                                          boolean firstFragment) throws IOException {

        boolean lastFragment = !fragmentedMigrationEnabled || !namespacesContext.hasNext();
        Operation operation = new MigrationOperation(migrationInfo, partitionStateVersion, migrationState,
                                                     firstFragment, lastFragment);

        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            Set<ServiceNamespace> namespaces = migrationState != null
                    ? migrationState.getNamespaceVersionMap().keySet() : Collections.<ServiceNamespace>emptySet();
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

            ReplicaFragmentMigrationState migrationState = createNextReplicaFragmentMigrationState();
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

    private ReplicaFragmentMigrationState createNextReplicaFragmentMigrationState() {
        assert fragmentedMigrationEnabled : "Fragmented migration should be enabled!";

        if (!namespacesContext.hasNext()) {
             return null;
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
                Collections.<ServiceNamespace>singleton(NonFragmentedServiceNamespace.INSTANCE);
        return createReplicaFragmentMigrationState(namespaces, operations);
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationStateFor(ServiceNamespace ns) {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Collection<String> serviceNames = namespacesContext.getServiceNames(ns);

        Collection<Operation> operations = createFragmentReplicationOperations(event, ns, serviceNames);
        return createReplicaFragmentMigrationState(singleton(ns), operations);
    }

    private ReplicaFragmentMigrationState createAllReplicaFragmentsMigrationState() {
        PartitionReplicationEvent event = getPartitionReplicationEvent();
        Collection<Operation> operations = createAllReplicationOperations(event);
        return createReplicaFragmentMigrationState(namespacesContext.allNamespaces, operations);
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationState(Collection<ServiceNamespace>
            namespaces, Collection<Operation> operations) {

        InternalPartitionService partitionService = getService();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();
        Map<ServiceNamespace, long[]> versions = new HashMap<ServiceNamespace, long[]>(namespaces.size());
        for (ServiceNamespace namespace : namespaces) {
            long[] v = versionManager.getPartitionReplicaVersions(getPartitionId(), namespace);
            versions.put(namespace, v);
        }

        return new ReplicaFragmentMigrationState(versions, operations);
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

    private final class SendNewMigrationFragmentRunnable implements PartitionSpecificRunnable, UrgentSystemOperation {

        @Override
        public int getPartitionId() {
            return MigrationRequestOperation.this.getPartitionId();
        }

        @Override
        public void run() {
            trySendNewFragment();
        }

    }

    private static class ServiceNamespacesContext {
        final Collection<ServiceNamespace> allNamespaces = new HashSet<ServiceNamespace>();
        final Map<ServiceNamespace, Collection<String>> namespaceToServices = new HashMap<ServiceNamespace, Collection<String>>();

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
                    serviceNames = new HashSet<String>(serviceNames);
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

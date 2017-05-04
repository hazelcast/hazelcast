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
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.SimpleExecutionCallback;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Migration request operation used before Hazelcast version 3.9
 * It runs on the migration source and transfers the whole partition data in one shot.
 */
public final class LegacyMigrationRequestOperation extends BaseMigrationSourceOperation {

    public LegacyMigrationRequestOperation() {
    }

    public LegacyMigrationRequestOperation(MigrationInfo migrationInfo, int partitionStateVersion) {
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
            Collection<Operation> tasks = prepareMigrationOperations();
            InternalPartitionServiceImpl partitionService = getService();
            long[] replicaVersions = partitionService.getPartitionReplicaVersionManager()
                    .getPartitionReplicaVersions(migrationInfo.getPartitionId(), NonFragmentedServiceNamespace.INSTANCE);
            invokeMigrationOperation(destination, replicaVersions, tasks);
            returnResponse = false;
        } catch (Throwable e) {
            logThrowable(e);
            setFailed();
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    private void invokeMigrationOperation(Address destination, long[] replicaVersions, Collection<Operation> tasks)
            throws IOException {

        LegacyMigrationOperation operation =
                new LegacyMigrationOperation(migrationInfo, replicaVersions, tasks, partitionStateVersion);

        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl partitionService = getService();

        nodeEngine.getOperationService()
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, operation, destination)
                .setExecutionCallback(new MigrationCallback(migrationInfo, this))
                .setResultDeserialized(true)
                .setCallTimeout(partitionService.getPartitionMigrationTimeout())
                .setTryCount(InternalPartitionService.MIGRATION_RETRY_COUNT)
                .setTryPauseMillis(InternalPartitionService.MIGRATION_RETRY_PAUSE)
                .setReplicaIndex(getReplicaIndex())
                .invoke();
    }

    private Collection<Operation> prepareMigrationOperations() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();

        PartitionReplicationEvent replicationEvent = getPartitionReplicationEvent();

        Collection<Operation> tasks = new LinkedList<Operation>();
        for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(MigrationAwareService.class)) {
            MigrationAwareService service = serviceInfo.getService();

            Operation op = service.prepareReplicationOperation(replicationEvent);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                tasks.add(op);
            }
        }

        return tasks;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.LEGACY_MIGRATION_REQUEST;
    }

    private static final class MigrationCallback extends SimpleExecutionCallback<Object> {

        final MigrationInfo migrationInfo;
        final LegacyMigrationRequestOperation op;

        private MigrationCallback(MigrationInfo migrationInfo, LegacyMigrationRequestOperation op) {
            this.migrationInfo = migrationInfo;
            this.op = op;
        }

        @Override
        public void notify(Object result) {
            op.completeMigration(Boolean.TRUE.equals(result));
        }
    }
}

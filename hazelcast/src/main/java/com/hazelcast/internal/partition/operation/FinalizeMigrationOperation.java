/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

// runs locally...
public final class FinalizeMigrationOperation extends AbstractOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    private final MigrationInfo migrationInfo;
    private final MigrationEndpoint endpoint;
    private final boolean success;

    public FinalizeMigrationOperation(MigrationInfo migrationInfo, MigrationEndpoint endpoint, boolean success) {
        this.migrationInfo = migrationInfo;
        this.endpoint = endpoint;
        this.success = success;
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl partitionService = getService();

        int partitionId = getPartitionId();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();

        PartitionMigrationEvent event = new PartitionMigrationEvent(endpoint, partitionId,
                endpoint == MigrationEndpoint.SOURCE
                        ? migrationInfo.getSourceCurrentReplicaIndex() : migrationInfo.getDestinationCurrentReplicaIndex(),
                endpoint == MigrationEndpoint.SOURCE
                        ? migrationInfo.getSourceNewReplicaIndex() : migrationInfo.getDestinationNewReplicaIndex());

        Collection<MigrationAwareService> migrationAwareServices = nodeEngine.getServices(MigrationAwareService.class);

        // Old backup owner is not notified about migration until migration
        // is committed on destination. This is the only place on backup owner
        // knows replica is moved away from itself.
        if (nodeEngine.getThisAddress().equals(migrationInfo.getSource())
                && migrationInfo.getSourceCurrentReplicaIndex() > 0) {
            // execute beforeMigration on old backup before commit/rollback
            for (MigrationAwareService service : migrationAwareServices) {
                beforeMigration(event, service);
            }
        }

        for (MigrationAwareService service : migrationAwareServices) {
            finishMigration(event, service);
        }

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        ILogger logger = getLogger();

        if (endpoint == MigrationEndpoint.SOURCE && success) {
            int keepReplicaIndex = migrationInfo.getSourceNewReplicaIndex();
            if (keepReplicaIndex < 0) {
                replicaManager.clearPartitionReplicaVersions(partitionId);
                if (logger.isFinestEnabled()) {
                    logger.finest("Replica versions are cleared in source after migration. partitionId=" + partitionId);
                }
            } else if (migrationInfo.getSourceCurrentReplicaIndex() != keepReplicaIndex && keepReplicaIndex > 1) {
                long[] versions = replicaManager.getPartitionReplicaVersions(partitionId);
                // No need to set versions back right now. actual version array is modified directly.
                Arrays.fill(versions, 0, keepReplicaIndex - 1, 0);

                if (logger.isFinestEnabled()) {
                    logger.finest("Replica versions are set after MOVE COPY BACK migration. partitionId="
                            + partitionId + " replica versions=" + Arrays.toString(versions));
                }
            }
        } else if (endpoint == MigrationEndpoint.DESTINATION && !success) {
            int destinationCurrentReplicaIndex = migrationInfo.getDestinationCurrentReplicaIndex();
            if (destinationCurrentReplicaIndex == -1) {
                replicaManager.clearPartitionReplicaVersions(partitionId);
                if (logger.isFinestEnabled()) {
                    logger.finest("Replica versions are cleared in destination after failed migration. partitionId="
                            + partitionId);
                }
            } else {
                long[] versions = replicaManager.getPartitionReplicaVersions(partitionId);
                int replicaOffset = migrationInfo.getDestinationCurrentReplicaIndex() <= 1 ? 1 : migrationInfo
                        .getDestinationCurrentReplicaIndex();
                // No need to set versions back right now. actual version array is modified directly.
                Arrays.fill(versions, 0, replicaOffset - 1, 0);

                if (logger.isFinestEnabled()) {
                    logger.finest("Replica versions are rolled back in destination after failed migration. partitionId="
                            + partitionId + " replica versions=" + Arrays.toString(versions));
                }
            }
        }

        if (success) {
            nodeEngine.onPartitionMigrate(migrationInfo);
        }
    }

    private void beforeMigration(PartitionMigrationEvent event, MigrationAwareService service) {
        try {
            service.beforeMigration(event);
        } catch (Throwable e) {
            getLogger().warning("Error before migration -> " + event, e);
        }
    }

    private void finishMigration(PartitionMigrationEvent event, MigrationAwareService service) {
        try {
            if (success) {
                service.commitMigration(event);
            } else {
                service.rollbackMigration(event);
            }
        } catch (Throwable e) {
            getLogger().warning("Error while finalizing migration -> " + event, e);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }
}

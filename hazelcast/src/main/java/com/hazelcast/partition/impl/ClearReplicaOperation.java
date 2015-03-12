/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;

// runs locally...
final class ClearReplicaOperation extends AbstractOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    private final int oldReplicaIndex;

    public ClearReplicaOperation(int oldReplicaIndex) {
        this.oldReplicaIndex = oldReplicaIndex;
    }

    @Override
    public void run() throws Exception {
        int partitionId = getPartitionId();
        InternalPartitionServiceImpl partitionService = getService();
        InternalPartitionImpl partition = partitionService.getPartitionImpl(partitionId);
        Address thisAddress = getNodeEngine().getThisAddress();

        // currentReplicaIndex == -1;               not owner/backup then clear data
        // currentReplicaIndex > oldReplicaIndex;   replica is demoted then clear data
        // currentReplicaIndex < oldReplicaIndex;   replica is promoted then keep data
        int currentReplicaIndex = partition.getReplicaIndex(thisAddress);
        if (currentReplicaIndex == -1 || currentReplicaIndex > oldReplicaIndex) {
            if (currentReplicaIndex == -1) {
                partitionService.cancelReplicaSync(partitionId);
            }
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Clearing partition replica... partition: " + partitionId
                        + ", old-replica: " + oldReplicaIndex + ", current-replica: " + currentReplicaIndex);
            }
            clearPartition(partitionId, partitionService);
        }
    }

    private void clearPartition(int partitionId, InternalPartitionServiceImpl partitionService) {
        NodeEngineImpl nodeEngine = partitionService.getNode().getNodeEngine();
        final Collection<MigrationAwareService> services = nodeEngine.getServices(MigrationAwareService.class);
        for (MigrationAwareService service : services) {
            try {
                service.clearPartitionReplica(partitionId);
            } catch (Throwable e) {
                logMigrationError(e);
            }
        }
        partitionService.clearPartitionReplicaVersions(partitionId);
    }

    private void logMigrationError(Throwable e) {
        ILogger logger = getLogger();
        logger.warning("While clearing partition data: " + getPartitionId(), e);
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

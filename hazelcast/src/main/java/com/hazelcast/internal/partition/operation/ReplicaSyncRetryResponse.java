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

import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public class ReplicaSyncRetryResponse extends Operation
        implements PartitionAwareOperation, BackupOperation, MigrationCycleOperation {

    public ReplicaSyncRetryResponse() {
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        final InternalPartitionServiceImpl partitionService = getService();
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();

        partitionService.getReplicaManager().clearReplicaSyncRequest(partitionId, replicaIndex);

        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
        Address thisAddress = getNodeEngine().getThisAddress();
        ILogger logger = getLogger();

        int currentReplicaIndex = partition.getReplicaIndex(thisAddress);
        if (currentReplicaIndex > 0) {
            if (logger.isFinestEnabled()) {
                logger.finest("Retrying replica sync request for partitionId=" + partitionId
                    + ", initial-replicaIndex=" + replicaIndex + ", current-replicaIndex=" + currentReplicaIndex);
            }
            partitionService.getReplicaManager().triggerPartitionReplicaSync(partitionId, currentReplicaIndex,
                    InternalPartitionService.REPLICA_SYNC_RETRY_DELAY);

        } else if (logger.isFinestEnabled()) {
            logger.finest("No need to retry replica sync request for partitionId=" + partitionId
                    + ", initial-replicaIndex=" + replicaIndex + ", current-replicaIndex=" + currentReplicaIndex);
        }
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
    }
}

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
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;

import java.io.IOException;

public class ReplicaSyncRetryResponse extends Operation
        implements PartitionAwareOperation, BackupOperation, UrgentSystemOperation, MigrationCycleOperation {

    public ReplicaSyncRetryResponse() {
    }

    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        final InternalPartitionServiceImpl partitionService = getService();
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();

        partitionService.clearReplicaSync(partitionId, replicaIndex);

        InternalPartitionImpl partition = partitionService.getPartitionImpl(partitionId);
        Address thisAddress = getNodeEngine().getThisAddress();
        ILogger logger = getLogger();

        int currentReplicaIndex = partition.getReplicaIndex(thisAddress);
        if (currentReplicaIndex > 0) {
            if (logger.isFinestEnabled()) {
                logger.finest("Retrying replica sync request for partition: " + partitionId
                    + ", initial-replica: " + replicaIndex + ", current-replica: " + currentReplicaIndex);
            }
            partitionService.triggerPartitionReplicaSync(partitionId, currentReplicaIndex,
                    InternalPartitionService.REPLICA_SYNC_RETRY_DELAY);

        } else if (logger.isFinestEnabled()) {
            logger.finest("No need to retry replica sync request for partition: " + partitionId
                    + ", initial-replica: " + replicaIndex + ", current-replica: " + currentReplicaIndex);
        }
    }

    public void afterRun() throws Exception {
    }

    public boolean returnsResponse() {
        return false;
    }

    public Object getResponse() {
        return null;
    }

    public boolean validatesTarget() {
        return false;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ReplicaSyncRetryResponse");
        sb.append("{partition=").append(getPartitionId());
        sb.append(", replica=").append(getReplicaIndex());
        sb.append('}');
        return sb.toString();
    }
}

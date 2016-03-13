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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionReplicaChangeReason;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Arrays;

// runs locally...
public final class ResetReplicaVersionOperation extends AbstractOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    private final PartitionReplicaChangeReason reason;

    private final boolean initialAssignment;

    public ResetReplicaVersionOperation(PartitionReplicaChangeReason reason, boolean initialAssignment) {
        this.reason = reason;
        this.initialAssignment = initialAssignment;
    }

    @Override
    public void run() throws Exception {
        int partitionId = getPartitionId();
        InternalPartitionService partitionService = getService();
        long[] versions = partitionService.getPartitionReplicaVersions(partitionId);
        // InternalPartitionService.getPartitionReplicaVersions() returns internal
        // version array, we need to clone it here
        versions = Arrays.copyOf(versions, InternalPartition.MAX_BACKUP_COUNT);

        final int replicaIndex = getReplicaIndex();
        final boolean setWaitingSyncFlag = !initialAssignment;

        if (setWaitingSyncFlag) {
            versions[replicaIndex - 1] = InternalPartition.SYNC_WAITING;

            if (getLogger().isFinestEnabled()) {
                getLogger().finest("SYNC_WAITING flag is set. partitionId=" + partitionId + " replicaIndex="
                        + replicaIndex + " replicaVersions=" + Arrays.toString(versions));
            }

            if (reason == PartitionReplicaChangeReason.ASSIGNMENT) {
                resetSyncWaitingVersionsAfterReplicaIndex(versions, replicaIndex);

                if (getLogger().isFinestEnabled()) {
                    getLogger().finest("SYNC_WAITING flags after replica index are reset. partitionId="
                            + partitionId + " replicaIndex=" + replicaIndex  + " replicaVersions=" + Arrays.toString(versions));
                }
            }
        }

        // clear and set replica versions back to ensure backup replica does not have
        // version numbers of prior replicas
        partitionService.clearPartitionReplicaVersions(partitionId);
        partitionService.setPartitionReplicaVersions(partitionId, versions, replicaIndex);
    }

    private void resetSyncWaitingVersionsAfterReplicaIndex(long[] versions, int replicaIndex) {
        for (int i = replicaIndex + 1; i <= versions.length; i++) {
            if (versions[i - 1] == InternalPartition.SYNC_WAITING) {
                versions[i - 1] = 0;
            }
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

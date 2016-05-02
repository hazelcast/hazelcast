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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.operation.CheckReplicaVersion;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

import static com.hazelcast.spi.partition.IPartitionService.SERVICE_NAME;

final class CheckReplicaVersionTask implements PartitionSpecificRunnable, UrgentSystemOperation {

    private static final int OPERATION_TRY_COUNT = 10;
    private static final int OPERATION_TRY_PAUSE_MILLIS = 250;

    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final int partitionId;
    private final int replicaIndex;
    private final ExecutionCallback callback;

    CheckReplicaVersionTask(NodeEngineImpl nodeEngine, InternalPartitionServiceImpl partitionService,
            int partitionId, int replicaIndex, ExecutionCallback callback) {
        this.nodeEngine = nodeEngine;
        this.partitionService = partitionService;
        this.partitionId = partitionId;

        if (replicaIndex < 1 || replicaIndex > InternalPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Replica index should be in range [1-"
                    + InternalPartition.MAX_BACKUP_COUNT + "]");
        }
        this.replicaIndex = replicaIndex;
        this.callback = callback;
    }

    @Override
    public void run() {
        int partitionId = getPartitionId();
        int replicaIndex = this.replicaIndex;
        InternalPartition partition = partitionService.getPartition(partitionId);
        Address target = partition.getReplicaAddress(replicaIndex);
        if (target == null) {
            notifyCallback(false);
            return;
        }

        invokeCheckReplicaVersion(partitionId, replicaIndex, target);
    }

    private void invokeCheckReplicaVersion(int partitionId, int replicaIndex, Address target) {
        long[] currentVersions = partitionService.getPartitionReplicaVersions(partitionId);
        long currentReplicaVersion = currentVersions[replicaIndex - 1];

        if (currentReplicaVersion <= 0) {
            notifyCallback(true);
            return;
        }

        CheckReplicaVersion op = new CheckReplicaVersion(currentReplicaVersion, shouldInvoke());
        op.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(SERVICE_NAME);
        OperationService operationService = nodeEngine.getOperationService();

        if (shouldInvoke()) {
            operationService.createInvocationBuilder(SERVICE_NAME, op, target)
                    .setExecutionCallback(callback)
                    .setTryCount(OPERATION_TRY_COUNT)
                    .setTryPauseMillis(OPERATION_TRY_PAUSE_MILLIS)
                    .invoke();
        } else {
            operationService.send(op, target);
        }
    }

    private void notifyCallback(boolean result) {
        if (shouldInvoke()) {
            callback.onResponse(result);
        }
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    private boolean shouldInvoke() {
        return callback != null;
    }
}

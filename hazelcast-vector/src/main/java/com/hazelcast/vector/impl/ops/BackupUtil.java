/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationRunnerImpl;

import static com.hazelcast.spi.impl.operationservice.Operation.GENERIC_PARTITION_ID;

final class BackupUtil {
    private BackupUtil() {
    }

    public static void handleBackupAndSendResponse(Operation op) {
        assert op.getPartitionId() != GENERIC_PARTITION_ID;
        OperationService operationService = op.getNodeEngine().getOperationService();
        OperationRunner runner = operationService.getOperationExecutor().getPartitionOperationRunners()[op.getPartitionId()];
        ((OperationRunnerImpl) runner).sendBackupsAndResponse(op);
    }

    /**
     * Marks partition replica as sync required. Does not trigger sync immediately,
     * it will be handled by regular anti-entropy task.
     * See also UtilSteps.HANDLE_ERROR.markReplicaAsSyncRequiredForBackupOps
     *
     * @apiNote should be invoked only from partition threads
     */
    public static void markReplicaAsSyncRequiredForBackupOps(Operation operation) {
        assert operation instanceof BackupOperation : "Should be invoked only for backup operations";

        PartitionReplicaVersionManager versionManager = getPartitionReplicaVersionManager(operation);

        int partitionId = operation.getPartitionId();
        ServiceNamespace namespace = versionManager.getServiceNamespace(operation);
        int replicaIndex = operation.getReplicaIndex();

        versionManager.markPartitionReplicaAsSyncRequired(partitionId, namespace, replicaIndex);
    }

    private static PartitionReplicaVersionManager getPartitionReplicaVersionManager(Operation operation) {
        var partitionService = (InternalPartitionService) operation.getNodeEngine().getPartitionService();
        return partitionService.getPartitionReplicaVersionManager();
    }
}

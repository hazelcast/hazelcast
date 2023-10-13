/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation.steps;

import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.operation.steps.engine.StepResponseUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.impl.OperationRunnerImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.function.Consumer;

public enum UtilSteps implements IMapOpStep {

    /**
     * Final step usually contains only operation response sending
     * functionality, but in some cases, before sending response, we
     * may want to inject some extra step and having this final step
     * can also help here by dispatching flow to these extra steps.
     * <p>
     * Example case: Doing compaction before sending response.
     */
    FINAL_STEP() {
        @Override
        public void runStep(State state) {
            // empty intentionally
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.SEND_RESPONSE;
        }
    },

    SEND_RESPONSE() {
        @Override
        public void runStep(State state) {
            StepResponseUtil.sendResponse(state);

            MapOperation operation = state.getOperation();
            operation.afterRunInternal();
            operation.disposeDeferredBlocks();

            if (operation instanceof BackupOperation) {
                // it can be possible that some operations marked
                // as BackupOperation but does not have backup.
                // see EvictBatchBackupOperation
                Consumer backupOpAfterRun = state.getBackupOpAfterRun();
                if (backupOpAfterRun != null) {
                    backupOpAfterRun.accept(operation);
                }
            }
        }

        @Override
        public Step nextStep(State state) {
            return null;
        }
    },


    HANDLE_ERROR() {
        @Override
        public void runStep(State state) {
            try {
                OperationRunnerImpl operationRunner = getPartitionOperationRunner(state);
                operationRunner.handleOperationError(state.getOperation(), state.getThrowable());
            } finally {
                state.setThrowable(null);
                markReplicaAsSyncRequiredForBackupOps(state);
            }
        }

        /**
         * When backup operation fails with an exception,
         * we need to mark replica as sync required
         * otherwise data inconsistencies can happen.
         */
        private void markReplicaAsSyncRequiredForBackupOps(State state) {
            MapOperation operation = state.getOperation();
            if (!(operation instanceof BackupOperation)) {
                return;
            }

            PartitionReplicaVersionManager versionManager = getPartitionReplicaVersionManager(state);

            int partitionId = state.getPartitionId();
            ServiceNamespace namespace = versionManager.getServiceNamespace(operation);
            int replicaIndex = operation.getReplicaIndex();

            versionManager.markPartitionReplicaAsSyncRequired(partitionId, namespace, replicaIndex);
        }

        private PartitionReplicaVersionManager getPartitionReplicaVersionManager(State state) {
            NodeEngineImpl nodeEngine = ((NodeEngineImpl) state.getRecordStore()
                    .getMapContainer().getMapServiceContext().getNodeEngine());
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            return partitionService.getPartitionReplicaVersionManager();
        }

        @Override
        public Step nextStep(State state) {
            return null;
        }
    },

    /**
     * When applied to a {@link MapOperation}, this {@link
     * #DIRECT_RUN_STEP} converts that operation into a Step
     * as a whole and makes that operation queued in {@link
     * com.hazelcast.map.impl.recordstore.DefaultRecordStore#offloadedOperations},
     * so that operations does not run in
     * parallel with other offloaded operations.
     */
    DIRECT_RUN_STEP {
        @Override
        public void runStep(State state) {
            MapOperation op = state.getOperation();
            op.runInternalDirect();
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };


    public static OperationRunnerImpl getPartitionOperationRunner(State state) {
        MapOperation operation = state.getOperation();
        return (OperationRunnerImpl) ((OperationServiceImpl) operation.getNodeEngine()
                .getOperationService()).getOperationExecutor()
                .getPartitionOperationRunners()[state.getPartitionId()];
    }
}

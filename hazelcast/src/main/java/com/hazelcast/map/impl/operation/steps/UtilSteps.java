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

import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.operation.steps.engine.StepResponseUtil;
import com.hazelcast.spi.impl.operationservice.impl.OperationRunnerImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

public enum UtilSteps implements IMapOpStep {

    SEND_RESPONSE() {
        @Override
        public void runStep(State state) {
            StepResponseUtil.sendResponse(state);

            MapOperation operation = state.getOperation();
            operation.afterRunInternal();
            operation.disposeDeferredBlocks();
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
            }
        }

        @Override
        public Step nextStep(State state) {
            return null;
        }
    };

    public static OperationRunnerImpl getPartitionOperationRunner(State state) {
        MapOperation operation = state.getOperation();
        return (OperationRunnerImpl) ((OperationServiceImpl) operation.getNodeEngine()
                .getOperationService()).getOperationExecutor()
                .getPartitionOperationRunners()[state.getPartitionId()];
    }
}

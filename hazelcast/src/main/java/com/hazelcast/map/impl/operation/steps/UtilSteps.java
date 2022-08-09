/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.operation.steps.engine.StepResponseUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

public enum UtilSteps implements Step<State> {

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
                handleOperationError(state);
            } finally {
                state.setThrowable(null);
            }
        }

        public void handleOperationError(State state) {
            MapOperation operation = state.getOperation();
            Throwable e = state.getThrowable();

            if (e instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            }
            try {
                operation.onExecutionFailure(e);
            } catch (Throwable t) {
                operation.getNodeEngine().getLogger(operation.getClass())
                        .warning("While calling 'operation.onFailure(e)'... op: "
                                + operation + ", error: " + e, t);
            }

            operation.logError(e);

            // A response is sent regardless of the
            // Operation.returnsResponse method because some operations do
            // want to send back a response, but they didn't want to send it
            // yet but they ran into some kind of error. If on the receiving
            // side no invocation is waiting, the response is ignored.
            sendResponseAfterOperationError(operation, e);
        }

        private void sendResponseAfterOperationError(Operation operation, Throwable e) {
            try {
                Node node = ((NodeEngineImpl) operation.getNodeEngine()).getNode();
                if (node.getState() != NodeState.SHUT_DOWN) {
                    operation.sendResponse(e);
                } else if (operation.executedLocally()) {
                    operation.sendResponse(new HazelcastInstanceNotActiveException());
                }
            } catch (Throwable t) {
                ILogger logger = operation.getNodeEngine().getLogger(operation.getClass());
                logger.warning("While sending op error... op: " + operation + ", error: " + e, t);
            }
        }

        @Override
        public Step nextStep(State state) {
            return null;
        }
    }
}

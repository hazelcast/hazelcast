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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.mapstore.writebehind.TxnReservedCapacityCounter;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.transaction.TransactionException;

import java.util.UUID;

public enum TxnRollbackOpSteps implements IMapOpStep {

    ROLLBACK() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();

            Data dataKey = state.getKey();
            long threadId = state.getThreadId();
            UUID ownerUuid = state.getOwnerUuid();
            UUID transactionId = state.getTxnId();
            MapOperation operation = state.getOperation();
            long callId = operation.getCallId();
            TxnReservedCapacityCounter wbqCapacityCounter
                    = operation.wbqCapacityCounter();

            wbqCapacityCounter.decrement(transactionId);
            if (recordStore.isLocked(dataKey)
                    && !recordStore.unlock(dataKey, ownerUuid, threadId, callId)) {
                throw new TransactionException("Lock is not owned by the transaction! Owner: "
                        + recordStore.getLockOwnerInfo(dataKey));
            }
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.SEND_RESPONSE;
        }
    };

    TxnRollbackOpSteps() {
    }
}

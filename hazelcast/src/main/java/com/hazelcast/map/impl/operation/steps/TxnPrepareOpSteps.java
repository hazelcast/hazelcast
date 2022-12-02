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
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.map.impl.mapstore.writebehind.TxnReservedCapacityCounter;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.transaction.TransactionException;

import java.util.UUID;

import static com.hazelcast.map.impl.tx.TxnPrepareOperation.LOCK_TTL_MILLIS;

public enum TxnPrepareOpSteps implements IMapOpStep {

    PREPARE() {
        @Override
        public void runStep(State state) {

            RecordStore recordStore = state.getRecordStore();

            Data dataKey = state.getKey();
            long threadId = state.getThreadId();
            UUID ownerUuid = state.getOwnerUuid();
            UUID transactionId = state.getTxnId();
            MapOperation operation = state.getOperation();
            TxnReservedCapacityCounter wbqCapacityCounter
                    = operation.wbqCapacityCounter();

            try {
                wbqCapacityCounter.increment(transactionId, false);
            } catch (ReachedMaxSizeException e) {
                throw new TransactionException(e);
            }

            if (!recordStore.extendLock(dataKey, ownerUuid, threadId, LOCK_TTL_MILLIS)) {
                ILogger logger = recordStore.getMapContainer().getMapStoreContext()
                        .getLogger(operation.getClass());
                if (logger.isFinestEnabled()) {
                    logger.finest("Locked: [" + recordStore.isLocked(dataKey)
                            + "], key: [" + dataKey + ']');
                }

                wbqCapacityCounter.decrement(transactionId);
                throw new TransactionException("Lock is not owned by the transaction! ["
                        + recordStore.getLockOwnerInfo(dataKey) + ']');
            }
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.SEND_RESPONSE;
        }
    };

    TxnPrepareOpSteps() {
    }
}

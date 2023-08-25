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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.transaction.TransactionException;

import java.util.UUID;

public enum TxnLockAndGetOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();

            Data dataKey = state.getKey();
            long threadId = state.getThreadId();
            long ttl = state.getTtl();
            UUID ownerUuid = state.getOwnerUuid();
            boolean blockReads = state.isBlockReads();
            long callId = state.getOperation().getCallId();

            if (!recordStore.txnLock(dataKey, ownerUuid, threadId, callId, ttl, blockReads)) {
                throw new TransactionException("Transaction couldn't obtain lock.");
            }

            GetOpSteps.READ.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null && state.isShouldLoad()
                    ? GetOpSteps.LOAD : GetOpSteps.RESPONSE;
        }
    };

    TxnLockAndGetOpSteps() {
    }
}

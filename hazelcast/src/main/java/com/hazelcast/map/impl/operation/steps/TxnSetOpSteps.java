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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;

import java.util.UUID;

public enum TxnSetOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();

            Data dataKey = state.getKey();
            long threadId = state.getThreadId();
            long version = state.getVersion();
            UUID ownerUuid = state.getOwnerUuid();
            long callId = state.getOperation().getCallId();

            recordStore.unlock(dataKey, ownerUuid, threadId, callId);

            Record record = recordStore.getRecordOrNull(dataKey, false);
            if (record == null || version == record.getVersion()) {
                PutOpSteps.READ.runStep(state);

                state.setEntryEventType(record == null
                        ? EntryEventType.ADDED : EntryEventType.UPDATED);
            }
        }

        @Override
        public Step nextStep(State state) {
            return PutOpSteps.READ.nextStep(state);
        }
    };

    TxnSetOpSteps() {
    }
}

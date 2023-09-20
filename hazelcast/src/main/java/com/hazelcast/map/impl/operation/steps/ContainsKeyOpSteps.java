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

import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;

public enum ContainsKeyOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            Record record = recordStore.getRecordOrNull(state.getKey(), false);

            if (record != null) {
                state.setOldValue(record.getValue());
                recordStore.accessRecord(state.getKey(), record, state.getNow());
            }
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null
                    ? ContainsKeyOpSteps.LOAD : UtilSteps.FINAL_STEP;
        }
    },

    LOAD() {
        @Override
        public boolean isLoadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            GetOpSteps.LOAD.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null
                    ? UtilSteps.FINAL_STEP : ContainsKeyOpSteps.ON_LOAD;
        }
    },

    ON_LOAD() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            Record record = ((DefaultRecordStore) recordStore).onLoadRecord(state.getKey(),
                    state.getOldValue(), false, state.getCallerAddress());
            record = recordStore.evictIfExpired(state.getKey(), state.getNow(), false) ? null : record;
            state.setOldValue(record == null ? null : record.getValue());

            if (record != null) {
                recordStore.accessRecord(state.getKey(), record, state.getNow());
            }
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    ContainsKeyOpSteps() {
    }
}

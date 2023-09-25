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

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;

public enum GetOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            Record record = recordStore.getRecordOrNull(state.getKey(), false);
            if (record != null) {
                Object oldValue = record.getValue();
                state.setOldValue(oldValue);
                state.setRecordExistsInMemory(true);
            }
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null ? GetOpSteps.LOAD : GetOpSteps.RESPONSE;
        }
    },

    LOAD() {
        @Override
        public boolean isLoadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            MapDataStore mapDataStore = state.getRecordStore().getMapDataStore();
            Object load = mapDataStore.load(state.getKey());
            state.setOldValue(load);
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null ? GetOpSteps.RESPONSE : GetOpSteps.ON_LOAD;
        }
    },

    ON_LOAD() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            Record record = ((DefaultRecordStore) recordStore).onLoadRecord(state.getKey(),
                    state.getOldValue(), false, state.getCallerAddress());
            record = recordStore.evictIfExpired(state.getKey(), state.getNow(), false)
                    ? null : record;
            state.setOldValue(record == null ? null : record.getValue());
        }

        @Override
        public Step nextStep(State state) {
            return GetOpSteps.RESPONSE;
        }
    },

    RESPONSE() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            Object oldValue = state.getOldValue();
            oldValue = mapServiceContext.interceptGet(mapContainer.getInterceptorRegistry(), oldValue);
            state.setOldValue(oldValue);

            Record record = recordStore.getRecord(state.getKey());
            if (record != null) {
                recordStore.accessRecord(state.getKey(), record, state.getNow());
            }
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    GetOpSteps() {
    }
}

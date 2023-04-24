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

public enum DeleteOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            Record record = recordStore.getRecordOrNull(state.getKey(), false);
            state.setOldValue(record == null ? null : record.getValue());
            state.setRecordExistsInMemory(record != null);

            if (record != null) {
                Object oldValue = record.getValue();
                oldValue = mapServiceContext.interceptRemove(mapContainer.getInterceptorRegistry(), oldValue);
                if (oldValue != null) {
                    state.setOldValue(oldValue);
                }
            }
        }

        @Override
        public Step nextStep(State state) {
            return DeleteOpSteps.DELETE;
        }
    },

    DELETE() {
        @Override
        public boolean isStoreStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            if (recordStore.persistenceEnabledFor(state.getCallerProvenance())) {
                MapDataStore mapDataStore = recordStore.getMapDataStore();
                mapDataStore.remove(state.getKey(), state.getNow(), state.getTxnId());
            }
        }

        @Override
        public Step nextStep(State state) {
            return state.isRecordExistsInMemory()
                    ? DeleteOpSteps.ON_DELETE : UtilSteps.FINAL_STEP;
        }
    },

    ON_DELETE() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            Record record = recordStore.getRecord(state.getKey());
            recordStore.removeRecord0(state.getKey(), record, false);
            recordStore.onStore(record);
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    DeleteOpSteps() {
    }
}

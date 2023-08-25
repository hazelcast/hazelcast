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

public enum RemoveOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            GetOpSteps.READ.runStep(state);

            Object oldValue = state.getOldValue();

            if (oldValue != null) {
                oldValue = mapServiceContext
                        .interceptRemove(mapContainer.getInterceptorRegistry(), oldValue);
                state.setOldValue(oldValue);
            }
        }

        @Override
        public Step nextStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            return !state.isRecordExistsInMemory() ? RemoveOpSteps.LOAD
                    : (recordStore.persistenceEnabledFor(state.getCallerProvenance())
                    ? RemoveOpSteps.DELETE : UtilSteps.FINAL_STEP);
        }
    },

    LOAD() {
        @Override
        public boolean isLoadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            Object oldValue = recordStore.loadValueOf(state.getKey());

            state.setOldValue(oldValue);
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null
                    ? UtilSteps.FINAL_STEP : RemoveOpSteps.DELETE;
        }
    },

    DELETE() {
        @Override
        public boolean isStoreStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            Object oldValue = state.getOldValue();
            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            if (oldValue != null && recordStore.persistenceEnabledFor(state.getCallerProvenance())) {
                MapDataStore mapDataStore = state.getRecordStore().getMapDataStore();
                mapDataStore.remove(state.getKey(), state.getNow(), state.getTxnId());

                recordStore.updateStatsOnRemove(state.getNow());
            }
        }

        @Override
        public RemoveOpSteps nextStep(State state) {
            return RemoveOpSteps.ON_DELETE;
        }
    },

    ON_DELETE() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            Record record = recordStore.getRecord(state.getKey());
            if (record == null) {
                return;
            }
            recordStore.removeRecord0(state.getKey(), record, false);
            recordStore.onStore(record);
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    RemoveOpSteps() {
    }
}

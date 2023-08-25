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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;

public enum RemoveIfSameOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
           RemoveOpSteps.READ.runStep(state);
        }

        @Override
        public RemoveIfSameOpSteps nextStep(State state) {
            return state.isRecordExistsInMemory()
                    ? RemoveIfSameOpSteps.ON_LOAD : RemoveIfSameOpSteps.LOAD;
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
            if (oldValue != null) {
                recordStore.getMapDataStore().remove(state.getKey(), state.getNow(), state.getTxnId());
                state.setOldValue(oldValue);
            } else {
                state.setResult(false);
            }
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null
                    ? UtilSteps.FINAL_STEP : RemoveIfSameOpSteps.ON_LOAD;
        }
    },

    ON_LOAD() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            SerializationService serializationService = mapServiceContext
                    .getNodeEngine().getSerializationService();
            ValueComparator valueComparator = recordStore.getValueComparator();

            Object oldValue = state.getOldValue();
            if (valueComparator.isEqual(state.getExpect(), oldValue, serializationService)) {
                mapServiceContext.interceptRemove(mapContainer.getInterceptorRegistry(), oldValue);
            } else {
                state.setResult(false);
                state.setStopExecution(true);
            }
        }

        @Override
        public Step nextStep(State state) {
            return state.isStopExecution()
                    ? UtilSteps.FINAL_STEP : RemoveIfSameOpSteps.DELETE;
        }
    },

    DELETE() {
        @Override
        public boolean isStoreStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            MapDataStore mapDataStore = state.getRecordStore().getMapDataStore();
            mapDataStore.remove(state.getKey(), state.getNow(), state.getTxnId());
        }

        @Override
        public RemoveIfSameOpSteps nextStep(State state) {
            return RemoveIfSameOpSteps.ON_DELETE;
        }
    },

    ON_DELETE() {
        @Override
        public void runStep(State state) {
            if (state.isRecordExistsInMemory()) {
                DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
                Record record = recordStore.getRecord(state.getKey());
                recordStore.removeRecord0(state.getKey(), record, false);
                recordStore.onStore(record);
                recordStore.updateStatsOnRemove(state.getNow());
            }

            state.setResult(true);
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    RemoveIfSameOpSteps() {
    }
}

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
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.EntryOperator;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;

public enum MultipleEntryOpSteps implements IMapOpStep {

    FIND_KEYS_TO_LOAD() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            IPartitionService partitionService = recordStore.getMapContainer()
                    .getMapServiceContext().getNodeEngine().getPartitionService();

            List<Data> keysToLoad = new ArrayList<>();
            Collection<Data> keys = state.getKeys();
            for (Data key : keys) {
                if (partitionService.getPartitionId(key) == state.getPartitionId()
                        && recordStore.getRecord(key) == null) {
                    keysToLoad.add(key);
                }
            }

            if (!keysToLoad.isEmpty()) {
                state.setKeysToLoad(keysToLoad);
            }
        }

        @Override
        public Step nextStep(State state) {
            if (!state.getKeysToLoad().isEmpty()) {
                return MultipleEntryOpSteps.LOAD_ALL;
            }
            return MultipleEntryOpSteps.PROCESS;
        }
    },

    LOAD_ALL() {
        @Override
        public boolean isLoadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            Collection<Data> keysToLoad = state.getKeysToLoad();
            Map loadedKeyValuePairs = state.getRecordStore().getMapDataStore().loadAll(keysToLoad);
            state.setLoadedKeyValuePairs(loadedKeyValuePairs);
        }

        @Override
        public Step nextStep(State state) {
            return state.getLoadedKeyValuePairs().isEmpty()
                    ? MultipleEntryOpSteps.PROCESS : MultipleEntryOpSteps.ON_LOAD_ALL;
        }
    },

    ON_LOAD_ALL() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            // create record for loaded records.
            Map<Object, Object> loadedKeyValuePairs = state.getLoadedKeyValuePairs();
            for (Map.Entry<Object, Object> entry : loadedKeyValuePairs.entrySet()) {
                Object key = entry.getKey();
                Object value = entry.getValue();
                if (key == null || value == null) {
                    continue;
                }

                ((DefaultRecordStore) recordStore)
                        .onLoadRecord(mapServiceContext.toData(key), value, false, state.getCallerAddress());
            }
        }

        @Override
        public Step nextStep(State state) {
            return MultipleEntryOpSteps.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();

            List<State> toStore = new ArrayList<>();
            List<State> toRemove = new ArrayList<>();

            state.setToStore(toStore);
            state.setToRemove(toRemove);

            MapEntries responses = new MapEntries(state.getKeys().size());
            state.setResult(responses);

            Collection<Data> keys = state.getKeys();
            for (Data key : keys) {
                Record record = recordStore.getRecord(key);

                // create specific state for this key
                State singleKeyState = new State(state);
                singleKeyState
                        .setKey(key)
                        .setOldValue(record == null ? null : record.getValue())
                        .setEntryOperator(operator(state.getOperation(),
                                state.getEntryProcessor(), state.getPredicate()));

                EntryOpSteps.PROCESS.runStep(singleKeyState);
                EntryOpSteps.DO_POST_OPERATE_OPS.runStep(singleKeyState);

                EntryEventType eventType = singleKeyState.getOperator().getEventType();
                if (eventType == null) {
                    Data result = singleKeyState.getOperator().getResult();
                    if (result != null) {
                        responses.add(singleKeyState.getKey(), result);
                    }
                } else {
                    switch (eventType) {
                        case ADDED:
                        case UPDATED:
                            toStore.add(singleKeyState);
                            break;
                        case REMOVED:
                            toRemove.add(singleKeyState);
                            break;
                        default:
                            throw new IllegalArgumentException("Unexpected event found:" + eventType);
                    }
                }
            }
        }

        @Override
        public Step nextStep(State state) {
            return !state.getToStore().isEmpty() || !state.getToRemove().isEmpty()
                    ? STORE_OR_DELETE : ON_STORE_OR_DELETE;
        }
    },

    STORE_OR_DELETE() {
        @Override
        public boolean isStoreStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            List<State> toStore = state.getToStore();
            for (State s : toStore) {
                PutOpSteps.STORE.runStep(s);
            }

            List<State> toRemove = state.getToRemove();
            for (State s : toRemove) {
                DeleteOpSteps.DELETE.runStep(s);
            }
        }

        @Override
        public Step nextStep(State state) {
            return ON_STORE_OR_DELETE;
        }
    },

    ON_STORE_OR_DELETE() {
        @Override
        public void runStep(State state) {
            List<State> toStore = state.getToStore();
            for (State s : toStore) {
                PutOpSteps.ON_STORE.runStep(s);

                EntryOperator operator = s.getOperator();
                operator.onAddedOrUpdated0(s.getNewValue());
                operator.doPostOperateOps0();

                Data result = s.getOperator().getResult();
                if (result != null) {
                    ((MapEntries) state.getResult()).add(s.getKey(), result);
                }
            }

            List<State> toRemove = state.getToRemove();
            for (State s : toRemove) {
                DeleteOpSteps.ON_DELETE.runStep(s);

                EntryOperator operator = s.getOperator();
                operator.onRemove0();
                operator.doPostOperateOps0();

                Data result = s.getOperator().getResult();
                if (result != null) {
                    ((MapEntries) state.getResult()).add(s.getKey(), result);
                }
            }
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    MultipleEntryOpSteps() {
    }
}

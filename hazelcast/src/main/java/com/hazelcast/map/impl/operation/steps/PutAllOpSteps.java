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
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.operation.PutAllOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

public enum PutAllOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());

            MapEntries mapEntries = state.getMapEntries();

            boolean triggerMapLoader = state.isTriggerMapLoader();
            boolean loadOldValue = triggerMapLoader
                    && ((PutAllOperation) state.getOperation()).isHasMapListener();
            List<Data> keysToLoadOldValue = loadOldValue ? new ArrayList<>() : Collections.emptyList();

            Map batchMap = createHashMap(mapEntries.size());
            List<Map.Entry<Data, Data>> entries = mapEntries.entries();
            for (Map.Entry<Data, Data> entry : entries) {
                if (loadOldValue && recordStore.getRecord(entry.getKey()) == null) {
                    keysToLoadOldValue.add(entry.getKey());
                }

                batchMap.put(entry.getKey(), entry.getValue());
            }

            state.setResult(batchMap);
            state.setKeysToLoad(keysToLoadOldValue);
        }

        @Override
        public Step nextStep(State state) {
            if (CollectionUtil.isNotEmpty(state.getKeysToLoad())) {
                return PutAllOpSteps.LOAD_ALL;
            }
            return PutAllOpSteps.STORE_ALL;
        }
    },

    LOAD_ALL() {
        @Override
        public boolean isLoadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            MultipleEntryOpSteps.LOAD_ALL.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            return STORE_ALL;
        }
    },

    STORE_ALL() {
        @Override
        public boolean isStoreStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            long expirationTime = recordStore.getExpirySystem().calculateExpirationTime(state.getTtl(),
                    state.getMaxIdle(), state.getNow(), state.getNow());
            // TODO storeAll can be more appropriate?
            Map result = (Map) state.getResult();
            Map newResult = new HashMap();
            for (Object e : result.entrySet()) {
                Map.Entry entry = (Map.Entry) e;

                Object storedValue = recordStore.getMapDataStore().add((Data) entry.getKey(), entry.getValue(),
                        expirationTime, state.getNow(), null);

                newResult.put(entry.getKey(), storedValue);
            }

            state.setResult(newResult);
        }

        @Override
        public Step nextStep(State state) {
            return PutAllOpSteps.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            boolean triggerMapLoader = state.isTriggerMapLoader();

            MapEntries mapEntries = state.getMapEntries();
            List<Map.Entry<Data, Data>> entries = mapEntries.entries();

            Map<Data, Object> oldValueByKey = new HashMap<>();
            Map result = (Map) state.getResult();
            State perKeyState = new State(state);
            boolean loadOldValue = triggerMapLoader
                    && ((PutAllOperation) state.getOperation()).isHasMapListener();
            Map loadedKeyValuePairs = state.getLoadedKeyValuePairs();

            for (Map.Entry<Data, Data> entry : entries) {
                Object oldValue = null;
                if (loadOldValue) {
                    oldValue = loadedKeyValuePairs.get(entry.getKey());
                }
                if (oldValue == null) {
                    Record record = recordStore.getRecord(entry.getKey());
                    if (record != null) {
                        oldValue = record.getValue();
                    }
                }

                perKeyState.setKey(entry.getKey())
                        .setOldValue(oldValue)
                        .setNewValue(result.get(entry.getKey()))
                        .setStaticPutParams(loadOldValue
                                ? StaticParams.PUT_PARAMS : StaticParams.SET_PARAMS);

                PutOpSteps.ON_STORE.runStep(perKeyState);

                if (loadOldValue && oldValue != null) {
                    // TODO why do we need to convert to heap data here?
                    oldValueByKey.put(perKeyState.getKey(),
                            mapServiceContext.toData(perKeyState.getOldValue()));
                }
            }

            state.setResult(oldValueByKey);
        }

        @Override
        public Step nextStep(State state) {
            return AFTER_RUN;
        }
    },

    AFTER_RUN() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
            PutAllOperation operation = (PutAllOperation) state.getOperation();

            List<Map.Entry<Data, Data>> entries = state.getMapEntries().entries();
            for (Map.Entry<Data, Data> entry : entries) {
                // it is possible that forced-eviction can delete some
                // entries, and we find some entries are missing.
                if (recordStore.getRecord(entry.getKey()) == null) {
                    continue;
                }

                Data dataKey = entry.getKey();
                Object newValue = entry.getValue();

                Data dataValue = operation.getValueOrPostProcessedValue(dataKey, (Data) newValue);
                mapServiceContext.interceptAfterPut(mapContainer.getInterceptorRegistry(), dataValue);

                if (operation.isHasMapListener()) {
                    Map<Data, Object> oldValueByKey = (Map<Data, Object>) state.getResult();
                    Object oldValue = oldValueByKey.get(dataKey);
                    EntryEventType eventType = (oldValue == null ? ADDED : UPDATED);
                    mapEventPublisher.publishEvent(state.getCallerAddress(), state.getOperation().getName(),
                            eventType, dataKey, oldValue, dataValue);
                }

                if (operation.isHasWanReplication()) {
                    operation.publishWanUpdate(dataKey, dataValue);
                }

                if (operation.isHasInvalidation()) {
                    operation.getInvalidationKeys().add(dataKey);
                }

                if (operation.isHasBackups()) {
                    operation.getBackupPairs().add(dataKey);
                    operation.getBackupPairs().add(dataValue);
                }

                operation.evict(dataKey);
            }
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    PutAllOpSteps() {
    }
}

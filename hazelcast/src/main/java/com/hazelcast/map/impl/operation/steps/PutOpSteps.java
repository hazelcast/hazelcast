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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;

import static com.hazelcast.map.impl.record.Record.UNSET;

public enum PutOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
            GetOpSteps.READ.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            if (!state.getStaticParams().isLoad()) {
                return PutOpSteps.PROCESS;
            }

            return state.getOldValue() == null
                    ? PutOpSteps.LOAD : PutOpSteps.PROCESS;
        }
    },

    LOAD() {
        @Override
        public boolean isLoadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            StaticParams staticParams = state.getStaticParams();
            if (staticParams.isPutVanilla()) {
                state.setOldValue(((DefaultRecordStore) state.getRecordStore())
                        .loadValueOf(state.getKey()));
            } else if (staticParams.isPutIfAbsent() || staticParams.isPutIfExists()) {
                GetOpSteps.LOAD.runStep(state);
            }
        }

        @Override
        public Step nextStep(State state) {
            return PutOpSteps.PROCESS;
        }
    },

    PROCESS() {
        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        @Override
        public void runStep(State state) {
            StaticParams staticParams = state.getStaticParams();
            Object newValue = state.getNewValue();

            // For method replace, if current value is not expected one, return.
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            if (!staticParams.isPutVanilla()) {
                if (staticParams.isPutIfAbsent()) {
                    Record record = recordStore.getRecord(state.getKey());
                    if (record == null && state.getOldValue() != null) {
                        record = ((DefaultRecordStore) recordStore).onLoadRecord(state.getKey(),
                                state.getOldValue(), false, state.getCallerAddress());
                    }

                    if (record != null) {
                        state.setOldValue(record.getValue());
                        state.setStopExecution(true);
                        return;
                    }
                } else if (staticParams.isPutIfExists()) {
                    Record record = recordStore.getRecord(state.getKey());
                    if (record == null && state.getOldValue() != null) {
                        record = ((DefaultRecordStore) recordStore).onLoadRecord(state.getKey(),
                                state.getOldValue(), false, state.getCallerAddress());
                    }

                    if (record == null) {
                        state.setOldValue(null);
                        state.setStopExecution(true);
                        state.setResult(false);
                        return;
                    }

                    newValue = staticParams.isSetTtl() ? record.getValue() : newValue;
                    state.setNewValue(newValue);
                    state.setOldValue(record.getValue());

                    if (staticParams.isPutIfEqual()
                            && !((DefaultRecordStore) recordStore).getValueComparator()
                            .isEqual(state.getExpect(), state.getOldValue(),
                                    mapServiceContext.getNodeEngine().getSerializationService())) {
                        state.setResult(false);
                        state.setStopExecution(true);
                        state.setOldValue(null);
                        return;
                    }
                }
            }

            // Intercept put on owner partition.
            newValue = mapServiceContext.interceptPut(mapContainer.getInterceptorRegistry(),
                    state.getOldValue(), newValue);
            state.setNewValue(newValue);
        }

        @Override
        public Step nextStep(State state) {
            if (state.isStopExecution()) {
                return UtilSteps.FINAL_STEP;
            }
            return state.getStaticParams().isTransient() ? ON_STORE : STORE;
        }
    },

    STORE() {
        @Override
        public boolean isStoreStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            assertWBStoreRunsOnPartitionThread(state);

            Object newValue = ((DefaultRecordStore) state.getRecordStore()).putIntoMapStore0(state.getKey(),
                    state.getNewValue(), state.getTtl(), state.getMaxIdle(), state.getNow(), state.getTxnId());
            state.setNewValue(newValue);
        }

        @Override
        public Step nextStep(State state) {
            return ON_STORE;
        }
    },

    ON_STORE() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            Record record = recordStore.getRecord(state.getKey());
            if (record == null) {
                record = recordStore.createRecord(state.getKey(), state.getNewValue(), state.getNow());
                recordStore.putMemory(record, state.getKey(), state.getOldValue(),
                        state.getTtl(), state.getMaxIdle(), UNSET,
                        state.getNow(), EntryEventType.ADDED, state.getStaticParams().isBackup());
            } else {
                // To heap copy and state object update are
                // needed for re-runs in case of forced eviction
                state.setOldValue(recordStore.getInMemoryFormat() == InMemoryFormat.OBJECT
                        ? record.getValue() : mapServiceContext.toData(record.getValue()));
                recordStore.updateRecord0(record, state.getNow(), state.getStaticParams().isCountAsAccess());
                Object oldValue = recordStore.updateMemory(record, state.getKey(), state.getOldValue(), state.getNewValue(),
                        state.isChangeExpiryOnUpdate(), state.getTtl(), state.getMaxIdle(), UNSET,
                        state.getNow(), state.getStaticParams().isBackup());
                state.setOldValue(oldValue);
            }

            state.setRecord(record);

            if (state.getStaticParams().isTransient()) {
                recordStore.getMapDataStore().addTransient(state.getKey(), state.getNow());
            }
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    PutOpSteps() {
    }
}

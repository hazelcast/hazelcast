/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Offloadable;
import com.hazelcast.map.impl.ExecutorStats;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.EntryOperation;
import com.hazelcast.map.impl.operation.EntryOperator;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.executionservice.impl.StatsAwareRunnable;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;

public enum EntryOpSteps implements IMapOpStep {

    EP_START() {
        @Override
        public void runStep(State state) {
            EntryOperator operator = operator(state.getOperation(), state.getEntryProcessor());
            operator.init(state.getKey(), state.getOldValue(), state.getNewValue(),
                    null, null, null, state.isChangeExpiryOnUpdate(), state.getTtl());
            state.setEntryOperator(operator);

            if (operator.belongsAnotherPartition(state.getKey())) {
                state.setStopExecution(true);
                return;
            }

            GetOpSteps.READ.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            if (state.isStopExecution()) {
                return EntryOpSteps.AFTER_RUN;
            }

            if (state.getOldValue() == null) {
                return EntryOpSteps.LOAD;
            }

            if (state.isEntryProcessorOffloadable()) {
                updateOldValueByConvertingItToHeapData(state);
                return EntryOpSteps.RUN_OFFLOADED_ENTRY_PROCESSOR;
            }
            return EntryOpSteps.PROCESS;
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
            if (state.getOldValue() != null) {
                return EntryOpSteps.ON_LOAD;
            }

            if (state.isEntryProcessorOffloadable()) {
                return EntryOpSteps.RUN_OFFLOADED_ENTRY_PROCESSOR;
            }

            return EntryOpSteps.PROCESS;
        }
    },

    RUN_OFFLOADED_ENTRY_PROCESSOR() {
        @Override
        public boolean isOffloadStep(State state) {
            return true;
        }

        @Override
        public String getExecutorName(State state) {
            return ((Offloadable) state.getEntryProcessor()).getExecutorName();
        }

        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            boolean statisticsEnabled = mapContainer.getMapConfig().isStatisticsEnabled();
            ExecutorStats executorStats = mapContainer.getMapServiceContext()
                    .getOffloadedEntryProcessorExecutorStats();

            EntryOpSteps.interceptGet(state);

            if (statisticsEnabled) {
                // When stats are enabled, to update
                // the stats wrap execution inside a
                // StatsAwareRunnable and run directly here
                StatsAwareRunnable statsAwareRunnable = new StatsAwareRunnable(() ->
                        runStepInternal(state), getExecutorName(state), executorStats);
                // directly run StatsAwareRunnable
                statsAwareRunnable.run();
            } else {
                runStepInternal(state);
            }
        }

        private void runStepInternal(State state) {
            EntryOperation operation = (EntryOperation) state.getOperation();
            EntryOperator entryOperator = operator(operation, state.getEntryProcessor())
                    .operateOnKeyValue(state.getKey(), state.getOldValue());
            state.setEntryOperator(entryOperator);
        }

        @Override
        public Step nextStep(State state) {
            EntryOperator entryOperator = state.getOperator();
            EntryEventType modificationType = entryOperator.getEventType();
            if (modificationType != null) {
                return DO_POST_OPERATE_OPS;
            }

            return UtilSteps.FINAL_STEP;
        }
    },

    ON_LOAD() {
        @Override
        public void runStep(State state) {
            GetOpSteps.ON_LOAD.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            if (state.isEntryProcessorOffloadable()) {
                updateOldValueByConvertingItToHeapData(state);
                return EntryOpSteps.RUN_OFFLOADED_ENTRY_PROCESSOR;
            }
            return EntryOpSteps.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            EntryOpSteps.interceptGet(state);

            EntryOperator entryOperator = state.getOperator();
            entryOperator.init(state.getKey(), state.getOldValue(),
                    null, null, null, recordStore.isLocked(state.getKey()),
                    state.isChangeExpiryOnUpdate(), state.getTtl());

            Object clonedOldValue = entryOperator.clonedOrRawOldValue();

            entryOperator.init(state.getKey(), clonedOldValue,
                    null, null, null, recordStore.isLocked(state.getKey()),
                    state.isChangeExpiryOnUpdate(), state.getTtl());

            if (!entryOperator.checkCanProceed()) {
                entryOperator.onTouched();
                state.setStopExecution(true);
                return;
            }
            entryOperator.operateOnKeyValueInternal();

            state.setEntryOperator(entryOperator);
        }

        @Override
        public Step nextStep(State state) {
            EntryOperator entryOperator = state.getOperator();
            if (!entryOperator.isDidMatchPredicate()) {
                return AFTER_RUN;
            }
            return DO_POST_OPERATE_OPS;
        }
    },

    DO_POST_OPERATE_OPS() {
        @Override
        public void runStep(State state) {
            EntryOperator entryOperator = state.getOperator();
            if (!entryOperator.isDidMatchPredicate()) {
                return;
            }
            MapContainer mapContainer = state.getRecordStore().getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            EntryEventType eventType = entryOperator.getEventType();
            if (eventType == null) {
                entryOperator.onTouched();
                state.setStopExecution(true);
            } else {
                switch (eventType) {
                    case ADDED:
                    case UPDATED:
                        if (eventType == EntryEventType.UPDATED) {
                            entryOperator.onTouched();
                        }
                        Object newValue = entryOperator.extractNewValue();
                        newValue = mapServiceContext.interceptPut(mapContainer.getInterceptorRegistry(),
                                entryOperator.getOldValueClone(), newValue);
                        state.setNewValue(newValue);
                        state.setTtl(entryOperator.getEntry().getNewTtl());
                        state.setChangeExpiryOnUpdate(entryOperator.getEntry().isChangeExpiryOnUpdate());
                        break;
                    case REMOVED:
                        // Almost the same as DeleteOpSteps.READ but without expiration,
                        // which was already checked when EP first obtained the entry.
                        // However, we handle missing record in case another overlapping operation evicted it.
                        Record record = state.getRecordStore().getRecord(state.getKey());
                        state.setOldValue(record == null ? null : record.getValue());
                        state.setRecordExistsInMemory(record != null);

                        if (record != null) {
                            Object oldValue = record.getValue();
                            oldValue = mapServiceContext.interceptRemove(mapContainer.getInterceptorRegistry(), oldValue);
                            if (oldValue != null) {
                                state.setOldValue(oldValue);
                            }
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected event found:" + eventType);
                }
            }
        }

        @Override
        public Step nextStep(State state) {
            EntryEventType eventType = state.getOperator().getEventType();
            if (eventType == null) {
                return AFTER_RUN;
            }

            switch (eventType) {
                case ADDED:
                case UPDATED:
                    return STORE;
                case REMOVED:
                    return DELETE;
                default:
                    throw new IllegalArgumentException("Unexpected event found:" + eventType);
            }
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

            PutOpSteps.STORE.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            return ON_STORE;
        }
    },

    ON_STORE() {
        @Override
        public void runStep(State state) {
            PutOpSteps.ON_STORE.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            return AFTER_RUN;
        }
    },

    DELETE() {
        @Override
        public boolean isStoreStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            assertWBStoreRunsOnPartitionThread(state);

            DeleteOpSteps.DELETE.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            return ON_DELETE;
        }
    },

    ON_DELETE() {
        @Override
        public void runStep(State state) {
            DeleteOpSteps.ON_DELETE.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            return AFTER_RUN;
        }
    },

    AFTER_RUN() {
        @Override
        public void runStep(State state) {
            EntryOperator operator = state.getOperator();
            EntryEventType eventType = operator.getEventType();
            if (eventType != null) {
                switch (eventType) {
                    case ADDED:
                    case UPDATED:
                        operator.onAddedOrUpdated0(state.getNewValue());
                        operator.doPostOperateOps0();
                        break;
                    case REMOVED:
                        operator.onRemove0();
                        operator.doPostOperateOps0();
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected eventType: " + eventType);
                }
            }
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    EntryOpSteps() {
    }

    private static void updateOldValueByConvertingItToHeapData(State state) {
        EntryOperation operation = (EntryOperation) state.getOperation();
        Object oldValueByInMemoryFormat = operation.copyOldValueToHeapWhenNeeded(state.getOldValue());
        state.setOldValue(oldValueByInMemoryFormat);
    }

    private static void interceptGet(State state) {
        MapContainer mapContainer = state.getRecordStore().getMapContainer();
        MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        Object value = mapServiceContext.interceptGet(mapContainer.getInterceptorRegistry(), state.getOldValue());
        state.setOldValue(value);
    }
}

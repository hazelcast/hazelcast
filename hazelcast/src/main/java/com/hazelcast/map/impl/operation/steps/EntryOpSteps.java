/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.EntryOperator;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;
import static com.hazelcast.map.impl.record.Record.UNSET;

public enum EntryOpSteps implements Step<State> {

    EP_START() {
        @Override
        public void runStep(State state) {
            EntryOperator operator = operator(state.getOperation(), state.getEntryProcessor());
            operator.init(state.getKey(), null, null,
                    null, null, null, UNSET);
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
            return state.getOldValue() == null
                    ? EntryOpSteps.LOAD : EntryOpSteps.PROCESS;
        }
    },

    LOAD() {
        @Override
        public boolean isOffloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            GetOpSteps.LOAD.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null
                    ? EntryOpSteps.PROCESS : EntryOpSteps.ON_LOAD;
        }
    },

    ON_LOAD() {
        @Override
        public void runStep(State state) {
            GetOpSteps.ON_LOAD.runStep(state);
        }

        @Override
        public Step nextStep(State state) {
            return EntryOpSteps.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            EntryOperator entryOperator = state.getOperator();
            entryOperator.init(state.getKey(), state.getOldValue(),
                    null, null, null, recordStore.isLocked(state.getKey()), state.getTtl());

            Object clonedOldValue = entryOperator.clonedOrRawOldValue();

            entryOperator.init(state.getKey(), clonedOldValue,
                    null, null, null, recordStore.isLocked(state.getKey()), state.getTtl());

            if (!entryOperator.checkCanProceed()) {
                entryOperator.onTouched();
                state.setStopExecution(true);
                return;
            }
            entryOperator.operateOnKeyValueInternal();

            if (!entryOperator.isDidMatchPredicate()) {
                return;
            }

            EntryEventType eventType = entryOperator.getEventType();
            if (eventType == null) {
                entryOperator.onTouched();
                state.setStopExecution(true);
            } else {
                switch (eventType) {
                    case ADDED:
                    case UPDATED:
                        state.setStaticPutParams(StaticParams.SET_WTH_NO_ACCESS_PARAMS);
                        if (eventType == EntryEventType.UPDATED) {
                            entryOperator.onTouched();
                        }
                        Object newValue = entryOperator.extractNewValue();
                        newValue = mapServiceContext.interceptPut(mapContainer.getInterceptorRegistry(),
                                state.getOldValue(), newValue);
                        state.setNewValue(newValue);
                        state.setTtl(entryOperator.getEntry().getNewTtl());
                        break;
                    case REMOVED:
                        DeleteOpSteps.READ.runStep(state);
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
        public boolean isOffloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
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
        public boolean isOffloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
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
            return UtilSteps.SEND_RESPONSE;
        }
    };

    EntryOpSteps() {
    }
}

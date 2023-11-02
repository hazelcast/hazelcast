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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.EntryOperator;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;

public enum PartitionWideEntryOpSteps implements IMapOpStep {

    PROCESS() {
        @Override
        public void runStep(State state) {
            /**
             * Tiered storage only supports global
             * indexes no partitioned index is supported.
             */
            if (isHDMap(state.getRecordStore())
                    && !isTieredStoreMap(state.getRecordStore())
                    && runWithPartitionedIndex(state)) {
                return;
            }
            runWithPartitionScan(state);
        }

        private boolean isHDMap(RecordStore<Record> recordStore) {
            return recordStore.getMapContainer().getMapConfig()
                    .getInMemoryFormat() == InMemoryFormat.NATIVE;
        }

        private boolean isTieredStoreMap(RecordStore<Record> recordStore) {
            return recordStore.getMapContainer().getMapConfig()
                    .getTieredStoreConfig().isEnabled();
        }

        private boolean runWithPartitionedIndex(State state) {
            Predicate predicate = state.getPredicate();
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            int partitionId = state.getPartitionId();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            QueryOptimizer queryOptimizer = mapServiceContext.getQueryOptimizer();

            // here we try to query the partitioned-index
            if (predicate == null) {
                return false;
            }

            // we use the partitioned-index to operate on the selected keys only
            IndexRegistry indexRegistry = mapContainer.getOrCreateIndexRegistry(partitionId);
            Iterable<QueryableEntry> entries = indexRegistry.query(queryOptimizer.optimize(predicate, indexRegistry), 1);
            if (entries == null) {
                return false;
            }

            List<State> toStore = new ArrayList<>();
            List<State> toRemove = new ArrayList<>();

            state.setToStore(toStore);
            state.setToRemove(toRemove);

            MapEntries responses = new MapEntries(recordStore.size());
            state.setResult(responses);

            // when NATIVE we can pass null as predicate since it's all
            // happening on partition thread so no data-changes may occur
            state.setPredicate(null);
            final HashSet keysFromIndex = new HashSet<>();
            entries.forEach(entry -> {
                keysFromIndex.add(entry.getKeyData());
                Record record = recordStore.getRecord(entry.getKeyData());
                processInternal(state, entry.getKeyData(), record);
            });

            state.setKeysFromIndex(keysFromIndex);

            return true;
        }

        private void runWithPartitionScan(State state) {
            RecordStore<Record> recordStore = state.getRecordStore();

            List<State> toStore = new ArrayList<>();
            List<State> toRemove = new ArrayList<>();

            state.setToStore(toStore);
            state.setToRemove(toRemove);

            MapEntries responses = new MapEntries(recordStore.size());
            state.setResult(responses);

            recordStore.forEach((key, record) -> {
                processInternal(state, toHeapData(key), record);

            }, false);
        }

        private void processInternal(State state, Data key, Record record) {
            State singleKeyState = new State(state);
            singleKeyState
                    .setKey(key)
                    .setOldValue(record == null ? null : record.getValue())
                    .setEntryOperator(operator(state.getOperation(),
                            state.getEntryProcessor(), state.getPredicate()));

            EntryOpSteps.PROCESS.runStep(singleKeyState);
            EntryOpSteps.DO_POST_OPERATE_OPS.runStep(singleKeyState);

            // state from main State object
            List<State> toStore = state.getToStore();
            List<State> toRemove = state.getToRemove();

            EntryEventType eventType = singleKeyState.getOperator().getEventType();
            if (eventType == null) {
                Data result = singleKeyState.getOperator().getResult();
                if (result != null) {
                    ((MapEntries) state.getResult()).add(singleKeyState.getKey(), result);
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

    PartitionWideEntryOpSteps() {
    }
}

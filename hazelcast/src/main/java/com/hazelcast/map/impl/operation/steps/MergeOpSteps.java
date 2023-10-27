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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.operation.MergeOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static com.hazelcast.core.EntryEventType.MERGED;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

public enum MergeOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
            MergeOperation operation = ((MergeOperation) state.getOperation());
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
            List<SplitBrainMergeTypes.MapMergeTypes<Object, Object>> mergingEntries = state.getMergingEntries();
            SplitBrainMergePolicy<Object, SplitBrainMergeTypes.MapMergeTypes<Object, Object>,
                    Object> mergePolicy = state.getMergePolicy();

            MergeOperation.checkMergePolicy(mapContainer, mergePolicy);

            Queue<InternalIndex> notMarkedIndexes = operation.beginIndexMarking();
            state.setNotMarkedIndexes(notMarkedIndexes);

            // key + oldValue + newValue + mergingEntry
            List outcomes = new ArrayList<>(NUMBER_OF_ITEMS * mergingEntries.size());

            for (SplitBrainMergeTypes.MapMergeTypes<Object, Object> mergingEntry : mergingEntries) {
                mergingEntry = (SplitBrainMergeTypes.MapMergeTypes<Object, Object>) serializationService
                        .getManagedContext().initialize(mergingEntry);
                mergePolicy = (SplitBrainMergePolicy<Object, SplitBrainMergeTypes.MapMergeTypes<Object, Object>, Object>)
                        serializationService.getManagedContext().initialize(mergePolicy);
                Data key = (Data) mergingEntry.getRawKey();
                Record record = recordStore.getRecordOrNull(key, state.getNow(), false);

                SplitBrainMergeTypes.MapMergeTypes<Object, Object> existingEntry = record != null
                        ? createMergingEntry(serializationService, key, record,
                        recordStore.getExpirySystem().getExpiryMetadata(key)) : null;

                Object oldValue = record == null ? null : record.getValue();
                Object newValue = mergePolicy.merge(mergingEntry, existingEntry);

                if (oldValue == null && newValue == null) {
                    continue;
                }

                outcomes.add(key);
                outcomes.add(oldValue);
                outcomes.add(newValue);
                outcomes.add(mergingEntry);
            }

            state.setResult(outcomes);
        }

        @Override
        public Step nextStep(State state) {
            boolean persistenceEnabled = ((DefaultRecordStore) state.getRecordStore())
                    .persistenceEnabledFor(state.getCallerProvenance());

            List outcomes = (List) state.getResult();
            return !outcomes.isEmpty() && persistenceEnabled
                    ? MergeOpSteps.STORE_OR_DELETE : MergeOpSteps.PROCESS;
        }
    },

    STORE_OR_DELETE() {
        @Override
        public boolean isStoreStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            // key + oldValue + newValue + mergingEntry
            List outcomes = (List) state.getResult();

            State perKeyState = new State(state);
            for (int i = 0; i < outcomes.size(); i += NUMBER_OF_ITEMS) {
                Object key = outcomes.get(i);

                assert key instanceof Data
                        : "Expect key instanceOf Data but found " + key;

                Object oldValue = outcomes.get(i + 1);
                Object newValue = outcomes.get(i + 2);

                perKeyState.setKey((Data) key)
                        .setOldValue(oldValue)
                        .setNewValue(newValue);

                if (oldValue == null && newValue != null
                        || oldValue != null && newValue != null) {
                    // put or update
                    PutOpSteps.STORE.runStep(perKeyState);
                    //set new value returned from map-store
                    outcomes.set(i + 2, perKeyState.getNewValue());
                } else if (oldValue != null && newValue == null) {
                    // remove
                    DeleteOpSteps.DELETE.runStep(perKeyState);
                }
            }

            state.setResult(outcomes);
        }

        @Override
        public Step nextStep(State state) {
            return MergeOpSteps.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            SerializationService serializationService = mapServiceContext
                    .getNodeEngine().getSerializationService();

            // key + oldValue + newValue + mergingEntry
            List outcomes = (List) state.getResult();
            State perKeyState = new State(state);
            for (int i = 0; i < outcomes.size(); i += NUMBER_OF_ITEMS) {
                Object key = outcomes.get(i);

                assert key instanceof Data
                        : "Expect key instanceOf Data but found " + key;

                Object oldValue = outcomes.get(i + 1);
                Object newValue = outcomes.get(i + 2);

                perKeyState.setKey((Data) key)
                        .setOldValue(oldValue)
                        .setNewValue(newValue)
                        .setStaticPutParams(StaticParams.PUT_PARAMS);

                if (oldValue == null && newValue != null
                        || oldValue != null && newValue != null) {

                    SplitBrainMergeTypes.MapMergeTypes mergingEntry
                            = (SplitBrainMergeTypes.MapMergeTypes) outcomes.get(i + 3);
                    // if same values, merge expiry and continue with next entry
                    if (recordStore.getValueComparator().isEqual(newValue, oldValue, serializationService)) {
                        Record record = recordStore.getRecord((Data) key);
                        if (record != null) {
                            recordStore.mergeRecordExpiration((Data) key, record, mergingEntry, state.getNow());
                        }
                        continue;
                    }

                    // put or update
                    PutOpSteps.ON_STORE.runStep(perKeyState);
                    Record record = recordStore.getRecord((Data) key);
                    recordStore.mergeRecordExpiration((Data) key, record, mergingEntry, state.getNow());
                } else if (oldValue != null && newValue == null) {
                    // remove
                    DeleteOpSteps.ON_DELETE.runStep(perKeyState);
                }
            }
        }

        @Override
        public Step nextStep(State state) {
            return RESPONSE;
        }
    },

    RESPONSE() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
            MergeOperation operation = ((MergeOperation) state.getOperation());

            List<Data> invalidationKeys = null;
            boolean hasMergedValues = false;

            List backupPairs = null;

            boolean hasMapListener = mapEventPublisher.hasEventListener(state.getOperation().getName());
            boolean hasWanReplication = mapContainer.getWanContext().isWanReplicationEnabled()
                    && !state.isDisableWanReplicationEvent();
            boolean hasBackups = mapContainer.getTotalBackupCount() > 0;
            boolean hasInvalidation = mapContainer.hasInvalidationListener();

            // key + oldValue + newValue + mergingEntry
            List outcomes = (List) state.getResult();

            if (hasBackups) {
                backupPairs = new ArrayList(2 * (outcomes.size() / NUMBER_OF_ITEMS));
            }

            if (hasInvalidation) {
                invalidationKeys = new ArrayList<>((outcomes.size() / NUMBER_OF_ITEMS));
            }

            for (int i = 0; i < outcomes.size(); i += NUMBER_OF_ITEMS) {
                hasMergedValues = true;

                Data dataKey = ((Data) outcomes.get(i));

                Object oldValue = outcomes.get(i + 1);
                // TODO can't we use this newValue directly.
                Object newValue = outcomes.get(i + 2);

                Data dataValue = operation.getValueOrPostProcessedValue(dataKey, operation.getValue(dataKey));
                mapServiceContext.interceptAfterPut(mapContainer.getInterceptorRegistry(), dataValue);

                if (hasMapListener) {
                    mapEventPublisher.publishEvent(state.getCallerAddress(), state.getOperation().getName(),
                            MERGED, dataKey, oldValue, dataValue);
                }

                if (hasWanReplication) {
                    operation.publishWanUpdate(dataKey, dataValue);
                }

                if (hasInvalidation) {
                    invalidationKeys.add(dataKey);
                }

                if (hasBackups) {
                    backupPairs.add(dataKey);
                    backupPairs.add(dataValue);
                }

                operation.evict(dataKey);
            }

            state.setBackupPairs(backupPairs);

            state.setResult(hasMergedValues);

            operation.invalidateNearCache(invalidationKeys);

            operation.finishIndexMarking(state.getNotMarkedIndexes());
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    private static final int NUMBER_OF_ITEMS = 4;

    MergeOpSteps() {
    }
}

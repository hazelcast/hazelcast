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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.operation.steps.UtilSteps.FINAL_STEP;
import static com.hazelcast.map.impl.operation.steps.engine.StepSupplier.injectCustomStepsToOperation;

public enum ClearOpSteps implements IMapOpStep {

    CLEAR_MEMORY() {
        @Override
        public void runStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            if (recordStore == null) {
                return;
            }
            recordStore.checkIfLoaded();

            state.setSizeBefore(recordStore.size());

            boolean tieredStorageEnabled = recordStore.isTieredStorageEnabled();
            ArrayList<Data> keys = new ArrayList<>(BATCH_SIZE);
            ArrayList<Record> records = tieredStorageEnabled ? null : new ArrayList<>(BATCH_SIZE);
            Iterator<Map.Entry<Data, Record>> iterator = recordStore.iterator();

            while (iterator.hasNext()) {
                Map.Entry<Data, Record> entry = iterator.next();
                Data dataKey = entry.getKey();
                Record record = entry.getValue();

                // skip locked keys
                if (!recordStore.isLocked(dataKey)) {
                    keys.add(tieredStorageEnabled ? toHeapData(dataKey) : dataKey);
                    if (!tieredStorageEnabled) {
                        records.add(record);
                    }
                }

                if (keys.size() == BATCH_SIZE) {
                    // Batch filling is completed
                    break;
                }
            }

            state.setKeys(keys);
            if (!tieredStorageEnabled) {
                state.setRecords(records);
            }
        }

        @Override
        public Step nextStep(State state) {
            return state.getRecordStore() == null
                    ? FINAL_STEP : ClearOpSteps.CLEAR_MAP_STORE;
        }
    },

    CLEAR_MAP_STORE() {
        @Override
        public boolean isStoreStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            Collection<Data> keys = state.getKeys();
            recordStore.getMapDataStore().removeAll(keys);
            recordStore.getMapDataStore().reset();
        }

        @Override
        public ClearOpSteps nextStep(State state) {
            return ClearOpSteps.ON_CLEAR;
        }
    },

    ON_CLEAR() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            int removedKeyCount = recordStore.removeBulk((ArrayList<Data>) state.getKeys(),
                    state.getRecords(), false);
            if (removedKeyCount > 0) {
                recordStore.updateStatsOnRemove(Clock.currentTimeMillis());
            }
            state.setResult(((Integer) state.getResult()) + removedKeyCount);
        }

        @Override
        public Step nextStep(State state) {
            RecordStore recordStore = state.getRecordStore();
            int currentSize = recordStore.size();
            int lockedSize = recordStore.getLockedEntryCount();

            // setSizeAfter to previous size to prevent forced compaction
            // we let force compaction only before going FINAL_STEP.
            boolean loopingToClear = currentSize - lockedSize > 0;
            state.setSizeAfter(loopingToClear ? state.getSizeBefore() : currentSize);
            Step injectBeforeStep = loopingToClear ? CLEAR_MEMORY : FINAL_STEP;
            return injectCustomStepsToOperation(state.getOperation(), injectBeforeStep);
        }
    };

    ClearOpSteps() {
    }
}

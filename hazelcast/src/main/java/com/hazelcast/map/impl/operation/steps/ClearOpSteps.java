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
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;

public enum ClearOpSteps implements IMapOpStep {

    CLEAR_MEMORY() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            if (recordStore == null) {
                state.setResult(0);
                return;
            }
            recordStore.checkIfLoaded();

            ArrayList<Data> keys = new ArrayList<>();
            ArrayList<Record> records = new ArrayList<>();
            // we don't remove locked keys. These are clearable records.
            recordStore.forEach(new BiConsumer<>() {
                final Set<Data> lockedKeySet = recordStore.getLockStore().getLockedKeys();

                @Override
                public void accept(Data dataKey, Record record) {
                    if (lockedKeySet != null && !lockedKeySet.contains(dataKey)) {
                        keys.add(recordStore.isTieredStorageEnabled() ? toHeapData(dataKey) : dataKey);
                        records.add(record);
                    }

                }
            }, false);

            state.setKeys(keys);
            state.setRecords(records);
        }

        @Override
        public Step nextStep(State state) {
            return state.getRecordStore() == null
                    ? UtilSteps.FINAL_STEP : ClearOpSteps.CLEAR_MAP_STORE;
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
            int removedKeyCount = recordStore.removeBulk((ArrayList<Data>) state.getKeys(), state.getRecords(), false);
            if (removedKeyCount > 0) {
                recordStore.updateStatsOnRemove(Clock.currentTimeMillis());
            }
            state.setResult(removedKeyCount);
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    ClearOpSteps() {
    }
}

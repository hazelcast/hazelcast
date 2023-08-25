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
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.operation.GetAllOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;

import java.util.List;
import java.util.Map;
import java.util.Set;

public enum GetAllOpSteps implements IMapOpStep {

    READ() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            GetAllOperation operation = (GetAllOperation) state.getOperation();
            Set<Data> partitionKeySet = operation.getPartitionKeySet((List<Data>) state.getKeys());

            MapEntries entries = recordStore.getInMemoryEntries(partitionKeySet, state.getNow());

            state.setKeysToLoad(partitionKeySet);
            state.setMapEntries(entries);
        }

        @Override
        public Step nextStep(State state) {
            return !state.getKeysToLoad().isEmpty()
                    ? GetAllOpSteps.LOAD_ALL : GetAllOpSteps.PROCESS;
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
            return GetAllOpSteps.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public void runStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());

            MapEntries mapEntries = state.getMapEntries();

            Map loaded = recordStore.loadEntries0(state.getLoadedKeyValuePairs(), state.getCallerAddress());

            recordStore.addToMapEntrySet(mapEntries, loaded);

            state.setMapEntries(mapEntries);
        }

        @Override
        public Step nextStep(State state) {
            return UtilSteps.FINAL_STEP;
        }
    };

    GetAllOpSteps() {
    }
}

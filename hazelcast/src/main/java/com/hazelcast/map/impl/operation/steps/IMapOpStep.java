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

import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;

import static com.hazelcast.spi.impl.executionservice.ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR;

/**
 * {@link Step} specialized for {@link
 * com.hazelcast.map.IMap} operations.
 */
public interface IMapOpStep extends Step<State> {

    /**
     * Decides when to offload based on configured map-store type.
     * <p>
     * Reasoning: Blindly offloading every step
     * may be less performant in all cases.
     * <ul>
     *     <li>Offloads all load and store steps
     *     when write-through map-store</li>
     *     <li>Offloads only load steps when write-behind map-store.
     *     <p>
     *     Since store steps in this case, have no-blocking behavior.
     *     <p>
     *     This is more performance friendly compared to offloading.
     *     </li>
     * </ul>
     */
    default boolean isOffloadStep(State state) {
        if (state.getRecordStore()
                .getMapDataStore().isNullImpl()) {
            // indicates no map-store is configured
            return false;
        }

        if (isLoadStep()) {
            return true;
        }

        if (isStoreStep()) {
            return !isWriteBehind(state);
        }

        return false;
    }

    default boolean isWriteBehind(State state) {
        return state.getRecordStore().getMapDataStore()
                instanceof WriteBehindStore;
    }

    default void assertWBStoreRunsOnPartitionThread(State state) {
        assert isStoreStep() && isWriteBehind(state)
                ? ThreadUtil.isRunningOnPartitionThread() : true;
    }

    /**
     * @return {@code true} when this step is loading
     * data via MapLoader, so it can be offloaded otherwise {@code false}
     */
    default boolean isLoadStep() {
        return false;
    }

    /**
     * @return {@code true} when this step is storing
     * data via MapStore, otherwise {@code false}
     */
    default boolean isStoreStep() {
        return false;
    }

    /**
     * @return name of the executor to execute this map operation step
     */
    default String getExecutorName(State state) {
        return MAP_STORE_OFFLOADABLE_EXECUTOR;
    }
}

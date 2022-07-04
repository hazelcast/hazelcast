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

package com.hazelcast.internal.monitor.impl;

/**
 * Provides per-operation statistics tracking for indexes.
 * <p>
 * Instances of implementing classes are assumed to be used by a single thread
 * at any given time. Therefore, implementations are not required to be
 * thread-safe.
 */
public interface IndexOperationStats {

    /**
     * Empty no-op index operation stats instance.
     */
    IndexOperationStats EMPTY = new IndexOperationStats() {
        @Override
        public long getEntryCountDelta() {
            return 0;
        }

        @Override
        public long getMemoryCostDelta() {
            return 0;
        }

        @Override
        public void onEntryAdded(Object addedValue) {
            // do nothing
        }

        @Override
        public void onEntryRemoved(Object removedValue) {
            // do nothing
        }
    };

    /**
     * Returns the indexed entry count delta produced as a result of an index
     * operation.
     */
    long getEntryCountDelta();

    /**
     * Returns the memory cost delta produced as a result of an index operation.
     */
    long getMemoryCostDelta();

    /**
     * Invoked by the associated index on every entry addition.
     *
     * @param addedValue the new added value.
     */
    void onEntryAdded(Object addedValue);

    /**
     * Invoked by the associated index on every entry removal.
     *
     * @param removedValue the old removed value.
     */
    void onEntryRemoved(Object removedValue);

}

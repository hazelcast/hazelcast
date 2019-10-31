/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
 * The implementation of operation index stats specialized for partition indexes.
 */
public class PartitionIndexOperationStats implements IndexOperationStats {

    private long entryCountDelta;

    @Override
    public long getEntryCountDelta() {
        return entryCountDelta;
    }

    @Override
    public long getMemoryCostDelta() {
        // Memory cost tracking for HD indexes is done on the native memory
        // allocator level.
        return 0;
    }

    @Override
    public void onEntryAdded(Object replacedValue, Object addedValue) {
        if (replacedValue == null) {
            ++entryCountDelta;
        }
    }

    @Override
    public void onEntryRemoved(Object removedValue) {
        if (removedValue != null) {
            --entryCountDelta;
        }
    }

    /**
     * Resets this stats instance to be ready for reuse.
     */
    public void reset() {
        entryCountDelta = 0;
    }

}

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

import com.hazelcast.query.impl.IndexHeapMemoryCostUtil;

/**
 * The implementation of operation index stats specialized for global indexes.
 */
public class GlobalIndexOperationStats implements IndexOperationStats {

    private long entryCountDelta;

    private long memoryCostDelta;

    @Override
    public long getEntryCountDelta() {
        return entryCountDelta;
    }

    @Override
    public long getMemoryCostDelta() {
        return memoryCostDelta;
    }

    @Override
    public void onEntryAdded(Object addedValue) {
        ++entryCountDelta;
        memoryCostDelta += IndexHeapMemoryCostUtil.estimateValueCost(addedValue);
    }

    @Override
    public void onEntryRemoved(Object removedValue) {
        --entryCountDelta;
        memoryCostDelta -= IndexHeapMemoryCostUtil.estimateValueCost(removedValue);
    }

}

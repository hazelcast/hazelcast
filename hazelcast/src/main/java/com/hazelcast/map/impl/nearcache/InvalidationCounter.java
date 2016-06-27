/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.HashUtil;

import java.util.concurrent.atomic.AtomicLongArray;

public class InvalidationCounter {

    private final int partitionCount;
    private final AtomicLongArray counters;

    public InvalidationCounter(int partitionCount) {
        this.partitionCount = partitionCount;
        this.counters = new AtomicLongArray(partitionCount);
    }

    public long getCount(Data key) {
        int slot = getSlot(key);
        return counters.get(slot);
    }

    public long increase(Data key) {
        int slot = getSlot(key);
        return counters.incrementAndGet(slot);
    }

    public boolean isStale(Data key, long previousCount) {
        int slot = getSlot(key);
        return counters.get(slot) > previousCount;
    }

    private int getSlot(Data key) {
        int hash = key.getPartitionHash();
        return HashUtil.hashToIndex(hash, partitionCount);
    }
}

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

package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;

/**
 * Size estimator for maps which have {@link InMemoryFormat#BINARY} memory-format.
 */
class BinaryMapSizeEstimator implements SizeEstimator {

    private volatile long size;

    BinaryMapSizeEstimator() {
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public void add(long size) {
        this.size += size;
    }

    @Override
    public void reset() {
        size = 0L;
    }

    @Override
    public long calculateSize(Object object) {

        if (object instanceof Data) {
            long keyCost = ((Data) object).getHeapCost();
            // CHM ref cost of key.
            keyCost += REFERENCE_COST_IN_BYTES;
            return keyCost;
        }

        if (object instanceof Record) {
            long recordCost = ((Record) object).getCost();
            // CHM ref cost of value.
            recordCost += REFERENCE_COST_IN_BYTES;
            // CHM ref costs of other.
            recordCost += REFERENCE_COST_IN_BYTES;
            recordCost += REFERENCE_COST_IN_BYTES;
            return recordCost;
        }

        return 0L;
    }
}

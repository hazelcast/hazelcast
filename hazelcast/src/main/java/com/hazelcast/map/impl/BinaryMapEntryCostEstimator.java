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

package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.internal.serialization.Data;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

/**
 * Owned entry cost estimator for maps which have {@link InMemoryFormat#BINARY} memory-format.
 */
class BinaryMapEntryCostEstimator
        implements EntryCostEstimator<Data, Record> {

    private static final int HASH_ENTRY_HASH_COST_IN_BYTES = REFERENCE_COST_IN_BYTES;

    private static final int HASH_ENTRY_VALUE_REF_COST_IN_BYTES = REFERENCE_COST_IN_BYTES;

    private static final int HASH_ENTRY_KEY_REF_COST_IN_BYTES = REFERENCE_COST_IN_BYTES;

    private static final int HASH_ENTRY_NEXT_REF_COST_IN_BYTES = REFERENCE_COST_IN_BYTES;

    private static final int HASH_ENTRY_COST_IN_BYTES =
            HASH_ENTRY_HASH_COST_IN_BYTES + HASH_ENTRY_KEY_REF_COST_IN_BYTES
                    + HASH_ENTRY_VALUE_REF_COST_IN_BYTES + HASH_ENTRY_NEXT_REF_COST_IN_BYTES;

    private volatile long estimate;

    BinaryMapEntryCostEstimator() {
    }

    @Override
    public long getEstimate() {
        return estimate;
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "We have the guarantee that only a single partition thread at any given time can change the volatile"
                    + " field, but multiple threads can read it.")
    @Override
    public void adjustEstimateBy(long adjustment) {
        this.estimate += adjustment;
    }

    @Override
    public void reset() {
        estimate = 0L;
    }

    @Override
    public long calculateValueCost(Record value) {
        return value.getCost();
    }

    public long calculateEntryCost(Data key, Record value) {
        long totalMapEntryCost = 0L;

        totalMapEntryCost += HASH_ENTRY_COST_IN_BYTES;
        totalMapEntryCost += key.getHeapCost();
        totalMapEntryCost += value.getCost();

        return totalMapEntryCost;
    }

}

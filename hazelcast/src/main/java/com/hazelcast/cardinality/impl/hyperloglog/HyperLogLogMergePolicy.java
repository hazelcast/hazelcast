/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cardinality.impl.hyperloglog;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;

import static com.hazelcast.cardinality.impl.CardinalityEstimatorDataSerializerHook.F_ID;
import static com.hazelcast.cardinality.impl.CardinalityEstimatorDataSerializerHook.HLL_MERGE_POLICY;

public class HyperLogLogMergePolicy
        implements SplitBrainMergePolicy, IdentifiedDataSerializable {

    public HyperLogLogMergePolicy() {
    }

    @Override
    public <K, V> V merge(SplitBrainMergeEntryView<K, V> mergingEntry, SplitBrainMergeEntryView<K, V> existingEntry) {
        if (!(mergingEntry.getValue() instanceof HyperLogLog)) {
            throw new IllegalArgumentException("Unsupported merging entries");
        }

        ((HyperLogLog) mergingEntry.getValue()).merge((HyperLogLog) existingEntry.getValue());
        return mergingEntry.getValue();
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getId() {
        return HLL_MERGE_POLICY;
    }
}

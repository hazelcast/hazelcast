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

package com.hazelcast.spi.merge;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.spi.impl.merge.AbstractSplitBrainMergePolicy;
import com.hazelcast.spi.impl.merge.CardinalityEstimatorMergingEntry;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CardinalityEstimatorMergeTypes;

import static com.hazelcast.spi.impl.merge.SplitBrainDataSerializerHook.HYPER_LOG_LOG;

/**
 * Only available for HyperLogLog backed {@link com.hazelcast.cardinality.CardinalityEstimator}.
 * <p>
 * Uses the default merge algorithm from HyperLogLog research, keeping the max register value of the two given instances.
 * The result should be the union to the two HyperLogLog estimations.
 *
 * @since 3.10
 */
public class HyperLogLogMergePolicy
        extends AbstractSplitBrainMergePolicy<HyperLogLog, CardinalityEstimatorMergeTypes, HyperLogLog> {

    public HyperLogLogMergePolicy() {
    }

    @Override
    public HyperLogLog merge(CardinalityEstimatorMergeTypes mergingValue,
                                                CardinalityEstimatorMergeTypes existingValue) {
        HyperLogLog merging = ((CardinalityEstimatorMergingEntry) mergingValue).getRawValue();
        if (existingValue == null) {
            return merging;
        }
        merging.merge(((CardinalityEstimatorMergingEntry) existingValue).getRawValue());
        return merging;
    }

    @Override
    public int getClassId() {
        return HYPER_LOG_LOG;
    }
}

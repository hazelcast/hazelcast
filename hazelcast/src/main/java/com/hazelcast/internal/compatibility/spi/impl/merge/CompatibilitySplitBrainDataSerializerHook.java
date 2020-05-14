/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.compatibility.spi.impl.merge;

import com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.ExpirationTimeMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.MergingExpirationTime;
import com.hazelcast.spi.merge.MergingHits;
import com.hazelcast.spi.merge.MergingLastAccessTime;
import com.hazelcast.spi.merge.MergingLastUpdateTime;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;

import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.SPLIT_BRAIN_DS_FACTORY;
import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.SPLIT_BRAIN_DS_FACTORY_ID;

/**
 * Data serializer hook containing (de)serialization information for communicating
 * with 4.x members over WAN.
 */
@SuppressWarnings("unused")
public final class CompatibilitySplitBrainDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = CompatibilityFactoryIdHelper.getFactoryId(SPLIT_BRAIN_DS_FACTORY, SPLIT_BRAIN_DS_FACTORY_ID);

    public static final int COLLECTION_MERGING_VALUE = 0;
    public static final int QUEUE_MERGING_VALUE = 1;
    public static final int ATOMIC_LONG_MERGING_VALUE = 2;
    public static final int ATOMIC_REFERENCE_MERGING_VALUE = 3;
    public static final int MAP_MERGING_ENTRY = 4;
    public static final int CACHE_MERGING_ENTRY = 5;
    public static final int MULTI_MAP_MERGING_ENTRY = 6;
    public static final int REPLICATED_MAP_MERGING_ENTRY = 7;
    public static final int RINGBUFFER_MERGING_ENTRY = 8;
    public static final int CARDINALITY_ESTIMATOR_MERGING_ENTRY = 9;
    public static final int SCHEDULED_EXECUTOR_MERGING_ENTRY = 10;

    public static final int DISCARD = 11;
    public static final int EXPIRATION_TIME = 12;
    public static final int HIGHER_HITS = 13;
    public static final int HYPER_LOG_LOG = 14;
    public static final int LATEST_ACCESS = 15;
    public static final int LATEST_UPDATE = 16;
    public static final int PASS_THROUGH = 17;
    public static final int PUT_IF_ABSENT = 18;

    private static final int LEN = PUT_IF_ABSENT + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case DISCARD:
                        return new DiscardMergePolicy<Object, MergingValue<Object>>();
                    case EXPIRATION_TIME:
                        return new ExpirationTimeMergePolicy<Object, MergingExpirationTime<Object>>();
                    case HIGHER_HITS:
                        return new HigherHitsMergePolicy<Object, MergingHits<Object>>();
                    case LATEST_ACCESS:
                        return new LatestAccessMergePolicy<Object, MergingLastAccessTime<Object>>();
                    case LATEST_UPDATE:
                        return new LatestUpdateMergePolicy<Object, MergingLastUpdateTime<Object>>();
                    case PASS_THROUGH:
                        return new PassThroughMergePolicy<Object, MergingValue<Object>>();
                    case PUT_IF_ABSENT:
                        return new PutIfAbsentMergePolicy<Object, MergingValue<Object>>();
                    default:
                        return null;
                }
            }
        };
    }
}

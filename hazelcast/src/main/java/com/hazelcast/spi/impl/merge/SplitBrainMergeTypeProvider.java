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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;

public final class SplitBrainMergeTypeProvider {

    private SplitBrainMergeTypeProvider() {
    }

    /**
     * Returns a class with the provided merge value types.
     * <p>
     * Is implemented by config classes of split-brain capable data structures.
     * <p>
     * Is used by the {@link com.hazelcast.internal.config.ConfigValidator} to
     * check if a data structure provides he required merge types of a
     * configured {@link SplitBrainMergePolicy}.
     *
     * @param config the configuration object
     * @return type of the provided merging values, e.g. a simple {@code MergingValue}
     * or a composition like {@code MergingEntry & MergingHits & MergingLastAccessTime}
     * @since 3.10
     */
    @SuppressWarnings({"checkstyle:returncount", "checkstyle:npathcomplexity"})
    public static Class<? extends MergingValue> getProvidedMergeTypes(Object config) {
        if (config instanceof CacheConfig) {
            return SplitBrainMergeTypes.CacheMergeTypes.class;
        }
        if (config instanceof CacheSimpleConfig) {
            return SplitBrainMergeTypes.CacheMergeTypes.class;
        }
        if (config instanceof CollectionConfig) {
            return SplitBrainMergeTypes.CollectionMergeTypes.class;
        }
        if (config instanceof MapConfig) {
            return SplitBrainMergeTypes.MapMergeTypes.class;
        }
        if (config instanceof MultiMapConfig) {
            return SplitBrainMergeTypes.MultiMapMergeTypes.class;
        }
        if (config instanceof QueueConfig) {
            return SplitBrainMergeTypes.QueueMergeTypes.class;
        }
        if (config instanceof ReplicatedMapConfig) {
            return SplitBrainMergeTypes.ReplicatedMapMergeTypes.class;
        }
        if (config instanceof RingbufferConfig) {
            return SplitBrainMergeTypes.RingbufferMergeTypes.class;
        }
        if (config instanceof ScheduledExecutorConfig) {
            return SplitBrainMergeTypes.ScheduledExecutorMergeTypes.class;
        }
        throw new IllegalArgumentException("Unsupported merge type");
    }
}

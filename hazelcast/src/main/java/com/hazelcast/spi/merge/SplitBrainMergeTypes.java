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

package com.hazelcast.spi.merge;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;
import com.hazelcast.spi.annotation.Beta;

import java.util.Collection;

/**
 * Collection of interfaces which define the provided merge types for each data structure.
 * <p>
 * Useful for implementors of {@link SplitBrainMergePolicy} to check on which data structures
 * their custom merge policy can be configured.
 * <p>
 * Will be returned by config classes which implement {@link SplitBrainMergeTypeProvider}
 * and used by the {@link com.hazelcast.internal.config.ConfigValidator} to check if a
 * configured {@link SplitBrainMergePolicy} is usable on its data structure.
 *
 * @since 3.10
 */
@Beta
public class SplitBrainMergeTypes {

    /**
     * Provided merge types of {@link com.hazelcast.core.IMap}.
     *
     * @since 3.10
     */
    @Beta
    public interface MapMergeTypes extends MergingEntry<Data, Data>, MergingCreationTime<Data>, MergingHits<Data>,
            MergingLastAccessTime<Data>, MergingLastUpdateTime<Data>, MergingTTL<Data>, MergingMaxIdle<Data>, MergingCosts<Data>,
            MergingVersion<Data>, MergingExpirationTime<Data>, MergingLastStoredTime<Data> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.cache.ICache}.
     *
     * @since 3.10
     */
    @Beta
    public interface CacheMergeTypes extends MergingEntry<Data, Data>, MergingCreationTime<Data>, MergingHits<Data>,
            MergingLastAccessTime<Data>, MergingExpirationTime<Data> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.core.ReplicatedMap}.
     *
     * @since 3.10
     */
    @Beta
    public interface ReplicatedMapMergeTypes extends MergingEntry<Object, Object>, MergingCreationTime<Object>,
            MergingHits<Object>, MergingLastAccessTime<Object>, MergingLastUpdateTime<Object>, MergingTTL<Object> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.core.MultiMap}.
     *
     * @since 3.10
     */
    @Beta
    public interface MultiMapMergeTypes extends MergingEntry<Data, Collection<Object>>, MergingCreationTime<Collection<Object>>,
            MergingHits<Collection<Object>>, MergingLastAccessTime<Collection<Object>>,
            MergingLastUpdateTime<Collection<Object>> {
    }

    /**
     * Provided merge types of collections ({@link com.hazelcast.core.ISet} and {@link com.hazelcast.core.IList}).
     *
     * @since 3.10
     */
    @Beta
    public interface CollectionMergeTypes extends MergingValue<Collection<Object>> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.core.IQueue}.
     *
     * @since 3.10
     */
    @Beta
    public interface QueueMergeTypes extends MergingValue<Collection<Object>> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.ringbuffer.Ringbuffer}.
     *
     * @since 3.10
     */
    @Beta
    public interface RingbufferMergeTypes extends MergingValue<RingbufferMergeData> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.core.IAtomicLong}.
     *
     * @since 3.10
     */
    @Beta
    public interface AtomicLongMergeTypes extends MergingValue<Long> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.core.IAtomicReference}.
     *
     * @since 3.10
     */
    @Beta
    public interface AtomicReferenceMergeTypes extends MergingValue<Object> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.scheduledexecutor.IScheduledExecutorService}.
     *
     * @since 3.10
     */
    @Beta
    public interface ScheduledExecutorMergeTypes extends MergingEntry<String, ScheduledTaskDescriptor> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.cardinality.CardinalityEstimator}.
     *
     * @since 3.10
     */
    @Beta
    public interface CardinalityEstimatorMergeTypes extends MergingEntry<String, HyperLogLog> {
    }
}

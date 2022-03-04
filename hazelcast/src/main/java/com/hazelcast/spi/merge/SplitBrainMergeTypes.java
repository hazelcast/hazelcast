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
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;

import java.util.Collection;

/**
 * Collection of interfaces which define the provided merge types for each data structure.
 * <p>
 * Useful for implementors of {@link SplitBrainMergePolicy} to check on which data structures
 * their custom merge policy can be configured.
 * <p>
 * Used by the {@link com.hazelcast.internal.config.ConfigValidator} to check if a
 * configured {@link SplitBrainMergePolicy} is usable on its data structure.
 *
 * @since 3.10
 */
public class SplitBrainMergeTypes {

    /**
     * Provided merge types of {@link IMap}.
     *
     * @param <K> key type
     * @param <V> value type
     * @since 3.10
     */
    public interface MapMergeTypes<K, V>
            extends MergingEntry<K, V>, MergingCreationTime, MergingHits, MergingLastAccessTime, MergingLastUpdateTime,
                    MergingTTL, MergingMaxIdle, MergingCosts, MergingVersion, MergingExpirationTime, MergingLastStoredTime {
    }

    /**
     * Provided merge types of {@link com.hazelcast.cache.ICache}.
     *
     * @param <K> key type
     * @param <V> value type
     * @since 3.10
     */
    public interface CacheMergeTypes<K, V>
            extends MergingEntry<K, V>, MergingCreationTime, MergingHits, MergingLastAccessTime, MergingExpirationTime {
    }

    /**
     * Provided merge types of {@link ReplicatedMap}.
     *
     * @param <K> key type
     * @param <V> value type
     * @since 3.10
     */
    public interface ReplicatedMapMergeTypes<K, V>
            extends MergingEntry<K, V>, MergingCreationTime, MergingHits, MergingLastAccessTime, MergingLastUpdateTime,
                    MergingTTL {
    }

    /**
     * Provided merge types of {@link MultiMap}.
     *
     * @param <K> key type
     * @param <V> value type
     * @since 3.10
     */
    public interface MultiMapMergeTypes<K, V>
            extends MergingEntry<K, Collection<V>>, MergingCreationTime, MergingHits, MergingLastAccessTime,
                    MergingLastUpdateTime {
    }

    /**
     * Provided merge types of collections ({@link ISet} and {@link IList}).
     *
     * @param <V> value type
     * @since 3.10
     */
    public interface CollectionMergeTypes<V> extends MergingValue<Collection<V>> {
    }

    /**
     * Provided merge types of {@link IQueue}.
     *
     * @param <V> value type
     * @since 3.10
     */
    public interface QueueMergeTypes<V> extends MergingValue<Collection<V>> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.ringbuffer.Ringbuffer}.
     *
     * @since 3.10
     */
    public interface RingbufferMergeTypes extends MergingValue<RingbufferMergeData> {
    }

    /**
     * Provided merge types of {@link IAtomicLong}.
     *
     * @since 3.10
     */
    public interface AtomicLongMergeTypes extends MergingValue<Long> {
    }

    /**
     * Provided merge types of {@link IAtomicReference}.
     *
     * @since 3.10
     */
    public interface AtomicReferenceMergeTypes extends MergingValue<Object> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.scheduledexecutor.IScheduledExecutorService}.
     *
     * @since 3.10
     */
    public interface ScheduledExecutorMergeTypes extends MergingEntry<String, ScheduledTaskDescriptor> {
    }

    /**
     * Provided merge types of {@link com.hazelcast.cardinality.CardinalityEstimator}.
     *
     * @since 3.10
     */
    public interface CardinalityEstimatorMergeTypes extends MergingEntry<String, HyperLogLog> {
    }
}

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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.ExpirationTimeMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.HyperLogLogMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.internal.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SPLIT_BRAIN_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SPLIT_BRAIN_DS_FACTORY_ID;

/**
 * Contains all the ID hooks for {@link IdentifiedDataSerializable} classes used by the split-brain framework.
 * <p>
 * {@link SplitBrainMergePolicy} classes are mapped here. This factory class is used by the
 * internal serialization system to create {@link IdentifiedDataSerializable} classes without using reflection.
 *
 * @since 3.10
 */
@SuppressWarnings("checkstyle:javadocvariable")
public final class SplitBrainDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SPLIT_BRAIN_DS_FACTORY, SPLIT_BRAIN_DS_FACTORY_ID);

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
        //noinspection unchecked
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[COLLECTION_MERGING_VALUE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionMergingValueImpl();
            }
        };
        constructors[QUEUE_MERGING_VALUE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueMergingValueImpl();
            }
        };
        constructors[ATOMIC_LONG_MERGING_VALUE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AtomicLongMergingValueImpl();
            }
        };
        constructors[ATOMIC_REFERENCE_MERGING_VALUE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AtomicReferenceMergingValueImpl();
            }
        };
        constructors[MAP_MERGING_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapMergingEntryImpl();
            }
        };
        constructors[CACHE_MERGING_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheMergingEntryImpl();
            }
        };
        constructors[MULTI_MAP_MERGING_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultiMapMergingEntryImpl();
            }
        };
        constructors[REPLICATED_MAP_MERGING_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedMapMergingEntryImpl();
            }
        };
        constructors[RINGBUFFER_MERGING_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RingbufferMergingValueImpl();
            }
        };
        constructors[CARDINALITY_ESTIMATOR_MERGING_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CardinalityEstimatorMergingEntry();
            }
        };
        constructors[SCHEDULED_EXECUTOR_MERGING_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ScheduledExecutorMergingEntryImpl();
            }
        };

        constructors[DISCARD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DiscardMergePolicy();
            }
        };
        constructors[EXPIRATION_TIME] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ExpirationTimeMergePolicy();
            }
        };
        constructors[HIGHER_HITS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HigherHitsMergePolicy();
            }
        };
        constructors[HYPER_LOG_LOG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HyperLogLogMergePolicy();
            }
        };
        constructors[LATEST_ACCESS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LatestAccessMergePolicy();
            }
        };
        constructors[LATEST_UPDATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LatestUpdateMergePolicy();
            }
        };
        constructors[PASS_THROUGH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PassThroughMergePolicy();
            }
        };
        constructors[PUT_IF_ABSENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutIfAbsentMergePolicy();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}

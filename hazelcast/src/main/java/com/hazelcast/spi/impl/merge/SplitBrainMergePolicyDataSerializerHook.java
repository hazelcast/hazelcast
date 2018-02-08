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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.HyperLogLogMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SimpleSplitBrainEntryView;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SPLIT_BRAIN_MERGE_POLICY_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SPLIT_BRAIN_MERGE_POLICY_DS_FACTORY_ID;

/**
 * Contains all the ID hooks for {@link IdentifiedDataSerializable} classes used by the split-brain framework.
 * <p>
 * {@link com.hazelcast.spi.SplitBrainMergePolicy} classes are mapped here. This factory class is used by the
 * internal serialization system to create {@link IdentifiedDataSerializable} classes without using reflection.
 *
 * @since 3.10
 */
@SuppressWarnings("checkstyle:javadocvariable")
public final class SplitBrainMergePolicyDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SPLIT_BRAIN_MERGE_POLICY_DS_FACTORY,
            SPLIT_BRAIN_MERGE_POLICY_DS_FACTORY_ID);

    public static final int ENTRY_VIEW = 0;
    public static final int DISCARD = 1;
    public static final int HIGHER_HITS = 2;
    public static final int HYPER_LOG_LOG = 3;
    public static final int LATEST_ACCESS = 4;
    public static final int LATEST_UPDATE = 5;
    public static final int PASS_THROUGH = 6;
    public static final int PUT_IF_ABSENT = 7;

    private static final int LEN = PUT_IF_ABSENT + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        //noinspection unchecked
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[ENTRY_VIEW] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SimpleSplitBrainEntryView();
            }
        };
        constructors[DISCARD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DiscardMergePolicy();
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

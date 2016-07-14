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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.REPLICATED_MAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.REPLICATED_MAP_DS_FACTORY_ID;

/**
 * This class contains all the ID hooks for IdentifiedDataSerializable classes used inside the replicated map.
 */
//Deactivated all checkstyle rules because those classes will never comply
//CHECKSTYLE:OFF
public class ReplicatedMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(REPLICATED_MAP_DS_FACTORY, REPLICATED_MAP_DS_FACTORY_ID);

    public static final int OP_CLEAR = 1;
    public static final int ENTRY_VIEW = 2;
    public static final int OP_REPLICATE_UPDATE = 3;
    public static final int OP_REPLICATE_UPDATE_TO_CALLER = 4;
    public static final int OP_PUT_ALL = 5;
    public static final int OP_PUT = 6;
    public static final int OP_REMOVE = 7;
    public static final int OP_SIZE = 8;
    public static final int OP_MERGE = 9;
    public static final int VERSION_RESPONSE_PAIR = 10;
    public static final int OP_GET = 11;

    private static final int LEN = OP_GET + 1;

    private static final DataSerializableFactory FACTORY = createFactoryInternal();

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return FACTORY;
    }

    private static DataSerializableFactory createFactoryInternal() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[OP_CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearOperation();
            }
        };
        constructors[ENTRY_VIEW] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedMapEntryView();
            }
        };
        constructors[OP_REPLICATE_UPDATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicateUpdateOperation();
            }
        };
        constructors[OP_REPLICATE_UPDATE_TO_CALLER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicateUpdateToCallerOperation();
            }
        };
        constructors[OP_PUT_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutAllOperation();
            }
        };
        constructors[OP_PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutOperation();
            }
        };
        constructors[OP_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveOperation();
            }
        };
        constructors[OP_SIZE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SizeOperation();
            }
        };
        constructors[OP_MERGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MergeOperation();
            }
        };
        constructors[VERSION_RESPONSE_PAIR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new VersionResponsePair();
            }
        };
        constructors[OP_GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GetOperation();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }

}

/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.mapreduce.impl.client.ClientMapReduceRequest;
import com.hazelcast.mapreduce.impl.operation.KeyValueMapReduceOperation;
import com.hazelcast.mapreduce.impl.operation.KeyValueMapReduceOperationFactory;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

public class MapReduceDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.MAP_REDUCE_DS_FACTORY, -23);

    public static final int KEY_VALUE_SOURCE_MAP = 0;
    public static final int KEY_VALUE_SOURCE_MULTIMAP = 1;
    public static final int KEY_VALUE_SOURCE_OPERATION = 2;
    public static final int KEY_VALUE_SOURCE_OPERATION_FACTORY = 3;
    public static final int CLIENT_MAP_REDUCE_REQUEST = 4;

    public static final int LEN = CLIENT_MAP_REDUCE_REQUEST + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable> constructors[] = new ConstructorFunction[LEN];
        constructors[KEY_VALUE_SOURCE_MAP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapKeyValueSource();
            }
        };
        constructors[KEY_VALUE_SOURCE_MULTIMAP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultiMapKeyValueSource();
            }
        };
        constructors[KEY_VALUE_SOURCE_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new KeyValueMapReduceOperation();
            }
        };
        constructors[KEY_VALUE_SOURCE_OPERATION_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new KeyValueMapReduceOperationFactory();
            }
        };
        constructors[CLIENT_MAP_REDUCE_REQUEST] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClientMapReduceRequest();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }

}

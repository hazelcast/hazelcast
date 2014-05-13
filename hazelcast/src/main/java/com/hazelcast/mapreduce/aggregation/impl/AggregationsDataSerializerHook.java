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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

/**
 * This class contains all the ID hooks for IdentifiedDataSerializable classes used for aggregations.
 */
//Deactivated all checkstyle rules because those classes will never comply
//CHECKSTYLE:OFF
public class AggregationsDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.AGGREGATIONS_DS_FACTORY, -24);

    public static final int SUPPLIER_CONSUMING_MAPPER = 0;
    public static final int ACCEPT_ALL_SUPPLIER = 1;

    private static final int LEN = ACCEPT_ALL_SUPPLIER + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[SUPPLIER_CONSUMING_MAPPER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SupplierConsumingMapper();
            }
        };
        constructors[ACCEPT_ALL_SUPPLIER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AcceptAllSupplier();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }
}

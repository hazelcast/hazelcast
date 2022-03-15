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

package com.hazelcast.internal.ascii;

import com.hazelcast.internal.ascii.memcache.MemcacheEntry;
import com.hazelcast.internal.ascii.rest.RestValue;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.TEXT_PROTOCOLS_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.TEXT_PROTOCOLS_DS_FACTORY_ID;

/**
 * DataSerializerHook for memcache &amp; REST protocol support classes
 */
public final class TextProtocolsDataSerializerHook implements DataSerializerHook {
    public static final int F_ID = FactoryIdHelper.getFactoryId(TEXT_PROTOCOLS_DS_FACTORY, TEXT_PROTOCOLS_DS_FACTORY_ID);

    public static final int MEMCACHE_ENTRY = 0;
    public static final int REST_VALUE = 1;

    public static final int LEN = REST_VALUE + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[MEMCACHE_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemcacheEntry();
            }
        };
        constructors[REST_VALUE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RestValue();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}

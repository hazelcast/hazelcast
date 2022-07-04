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

package com.hazelcast.json.internal;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JSON_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JSON_DS_FACTORY_ID;

/**
 * Serialization hook for JSON classes.
 */
@SuppressWarnings("checkstyle:JavadocVariable")
public final class JsonDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(JSON_DS_FACTORY, JSON_DS_FACTORY_ID);

    public static final int JSON_SCHEMA_NAME_VALUE = 0;
    public static final int JSON_SCHEMA_TERMINAL_NODE = 1;
    public static final int JSON_SCHEMA_STRUCT_NODE = 2;

    private static final int LEN = JSON_SCHEMA_STRUCT_NODE + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[JSON_SCHEMA_NAME_VALUE] = arg -> new JsonSchemaNameValue();
        constructors[JSON_SCHEMA_TERMINAL_NODE] = arg -> new JsonSchemaTerminalNode();
        constructors[JSON_SCHEMA_STRUCT_NODE] = arg -> new JsonSchemaStructNode();

        return new ArrayDataSerializableFactory(constructors);
    }

}


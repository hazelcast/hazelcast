/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonQueryFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonValueFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonParseFunction;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JET_SQL_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JET_SQL_DS_FACTORY_ID;

/**
 * Serialization hook for Jet SQL engine classes.
 */
public class JetSqlSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(JET_SQL_DS_FACTORY, JET_SQL_DS_FACTORY_ID);

    public static final int JSON_QUERY = 0;
    public static final int JSON_PARSE = 1;
    public static final int JSON_VALUE = 2;

    public static final int LEN = JSON_VALUE + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[JSON_QUERY] = arg -> new JsonQueryFunction();
        constructors[JSON_PARSE] = arg -> new JsonParseFunction();
        constructors[JSON_VALUE] = arg -> new JsonValueFunction();

        return new ArrayDataSerializableFactory(constructors);
    }
}

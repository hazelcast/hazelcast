/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.util.ConstructorFunction;
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

    private static final SqlSerializationFactory SERIALIZATION_FACTORY = lookupSerializationFactory();

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {

        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[JSON_QUERY] = arg -> SERIALIZATION_FACTORY.createJsonQueryFunction();
        constructors[JSON_PARSE] = arg -> SERIALIZATION_FACTORY.createJsonParseFunction();
        constructors[JSON_VALUE] = arg -> SERIALIZATION_FACTORY.createJsonValueFunction();

        return new ArrayDataSerializableFactory(constructors);
    }

    private static SqlSerializationFactory lookupSerializationFactory() {
        try {
            final Class<?> clazz = Class.forName("com.hazelcast.jet.sql.impl.JetSqlSerializationFactory");
            return (SqlSerializationFactory) clazz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ignored) {
            return new NoopSqlSerializationFactory();
        }
    }
}

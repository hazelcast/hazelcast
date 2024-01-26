/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.RowValue;

import java.util.function.Supplier;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SQL_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SQL_DS_FACTORY_ID;

/**
 * Serialization hook for SQL classes.
 */
public class SqlDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SQL_DS_FACTORY, SQL_DS_FACTORY_ID);

    public static final int QUERY_DATA_TYPE = 0;
    public static final int QUERY_ID = 1;
    public static final int MAPPING = 57;
    public static final int MAPPING_FIELD = 58;
    public static final int VIEW = 61;
    public static final int TYPE = 63;
    public static final int TYPE_FIELD = 64;
    public static final int ROW_VALUE = 66;
    public static final int QUERY_DATA_TYPE_FIELD = 67;
    public static final int PREDEFINED_QUERY_DATA_TYPE_BASE = 68;
    /**
     * There can be at most {@value com.hazelcast.sql.impl.type.converter.Converters#MAX_CONVERTER_COUNT}
     * predefined QueryDataType's. Currently, there are 26 of them.
     */
    public static final int PREDEFINED_QUERY_DATA_TYPE_END = 167;

    public static final int LEN = PREDEFINED_QUERY_DATA_TYPE_END + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[QUERY_ID] = QueryId::new;
        constructors[ROW_VALUE] = RowValue::new;

        // other constructors are added in JetSqlSerializerHook.afterFactoriesCreated()

        return new ArrayDataSerializableFactory(constructors);
    }
}

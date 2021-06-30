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
import com.hazelcast.jet.sql.impl.connector.map.MapIndexScanP;
import com.hazelcast.jet.sql.impl.schema.Mapping;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JET_SQL_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JET_SQL_DS_FACTORY_ID;

/**
 * Serialization hook for Jet SQL engine classes.
 */
public class JetSqlSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(JET_SQL_DS_FACTORY, JET_SQL_DS_FACTORY_ID);

    public static final int MAPPING = 0;
    public static final int MAPPING_FIELD = 1;

    public static final int IMAP_INDEX_SCAN_PROCESSOR_META_SUPPLIER = 10;
    public static final int IMAP_INDEX_SCAN_PROCESSOR_SUPPLIER = 11;

    public static final int LEN = IMAP_INDEX_SCAN_PROCESSOR_SUPPLIER + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[MAPPING] = arg -> new Mapping();
        constructors[MAPPING_FIELD] = arg -> new MappingField();

        constructors[IMAP_INDEX_SCAN_PROCESSOR_META_SUPPLIER] =
                arg -> new MapIndexScanP.MapIndexScanProcessorMetaSupplier();

        constructors[IMAP_INDEX_SCAN_PROCESSOR_SUPPLIER] =
                arg -> new MapIndexScanP.MapIndexScanProcessorSupplier();

        return new ArrayDataSerializableFactory(constructors);
    }
}

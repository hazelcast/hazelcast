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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.jet.sql.impl.connector.map.OnHeapMapIndexScanP;
import com.hazelcast.jet.sql.impl.connector.map.OnHeapMapScanP;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JET_SQL_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JET_SQL_DS_FACTORY_ID;

/**
 * Serialization hook for Jet SQL engine classes.
 */
public class JetSqlSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(JET_SQL_DS_FACTORY, JET_SQL_DS_FACTORY_ID);

    public static final int IMAP_SCAN_PROCESSOR = 1;
    public static final int IMAP_SCAN_PROCESSOR_META_SUPPLIER = 2;
    public static final int IMAP_SCAN_PROCESSOR_SUPPLIER = 3;
    public static final int IMAP_INDEX_SCAN_PROCESSOR = 4;
    public static final int IMAP_INDEX_SCAN_PROCESSOR_META_SUPPLIER = 5;
    public static final int IMAP_INDEX_SCAN_PROCESSOR_SUPPLIER = 6;

    public static final int LEN = IMAP_INDEX_SCAN_PROCESSOR_SUPPLIER + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[IMAP_SCAN_PROCESSOR] = arg -> new OnHeapMapScanP();
        constructors[IMAP_SCAN_PROCESSOR_META_SUPPLIER] = arg -> new OnHeapMapScanP.OnHeapMapScanMetaSupplier();
        constructors[IMAP_SCAN_PROCESSOR_SUPPLIER] = arg -> new OnHeapMapScanP.OnHeapMapScanSupplier();

        constructors[IMAP_INDEX_SCAN_PROCESSOR] = arg -> new OnHeapMapIndexScanP();
        constructors[IMAP_INDEX_SCAN_PROCESSOR_META_SUPPLIER] = arg -> new OnHeapMapIndexScanP.OnHeapMapIndexScanMetaSupplier();
        constructors[IMAP_INDEX_SCAN_PROCESSOR_SUPPLIER] = arg -> new OnHeapMapIndexScanP.OnHeapMapIndexScanSupplier();

        return new ArrayDataSerializableFactory(constructors);
    }
}

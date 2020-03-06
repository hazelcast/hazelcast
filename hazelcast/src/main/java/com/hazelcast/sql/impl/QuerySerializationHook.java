/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.operation.QueryBatchExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryCancelOperation;
import com.hazelcast.sql.impl.operation.QueryCheckOperation;
import com.hazelcast.sql.impl.operation.QueryCheckResponseOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryFlowControlExchangeOperation;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.JoinRow;
import com.hazelcast.sql.impl.row.ListRowBatch;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SQL_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SQL_DS_FACTORY_ID;

/**
 * Serialization hook for SQL classes.
 */
public class QuerySerializationHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SQL_DS_FACTORY, SQL_DS_FACTORY_ID);

    public static final int ROW_HEAP = 0;
    public static final int ROW_JOIN = 1;
    public static final int ROW_BATCH_LIST = 2;
    public static final int ROW_BATCH_EMPTY = 3;

    public static final int OPERATION_EXECUTE = 4;
    public static final int OPERATION_BATCH = 5;
    public static final int OPERATION_FLOW_CONTROL = 6;
    public static final int OPERATION_CANCEL = 7;
    public static final int OPERATION_CHECK = 8;
    public static final int OPERATION_CHECK_RESPONSE = 9;

    public static final int QUERY_ID = 10;

    public static final int LEN = QUERY_ID + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[ROW_HEAP] = arg -> new HeapRow();
        constructors[ROW_JOIN] = arg -> new JoinRow();
        constructors[ROW_BATCH_LIST] = arg -> new ListRowBatch();
        constructors[ROW_BATCH_EMPTY] = arg -> EmptyRowBatch.INSTANCE;

        constructors[OPERATION_EXECUTE] = arg -> new QueryExecuteOperation();
        constructors[OPERATION_BATCH] = arg -> new QueryBatchExchangeOperation();
        constructors[OPERATION_FLOW_CONTROL] = arg -> new QueryFlowControlExchangeOperation();
        constructors[OPERATION_CANCEL] = arg -> new QueryCancelOperation();
        constructors[OPERATION_CHECK] = arg -> new QueryCheckOperation();
        constructors[OPERATION_CHECK_RESPONSE] = arg -> new QueryCheckResponseOperation();

        constructors[QUERY_ID] = arg -> new QueryId();

        return new ArrayDataSerializableFactory(constructors);
    }
}

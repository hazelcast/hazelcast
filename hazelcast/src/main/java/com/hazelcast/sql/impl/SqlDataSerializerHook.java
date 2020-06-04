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
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.predicate.IsNullPredicate;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.operation.QueryBatchExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryCancelOperation;
import com.hazelcast.sql.impl.operation.QueryCheckOperation;
import com.hazelcast.sql.impl.operation.QueryCheckResponseOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFragment;
import com.hazelcast.sql.impl.operation.QueryFlowControlExchangeOperation;
import com.hazelcast.sql.impl.plan.node.FilterPlanNode;
import com.hazelcast.sql.impl.plan.node.ProjectPlanNode;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.RootSendPlanNode;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.JoinRow;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SQL_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SQL_DS_FACTORY_ID;

/**
 * Serialization hook for SQL classes.
 */
public class SqlDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SQL_DS_FACTORY, SQL_DS_FACTORY_ID);

    public static final int QUERY_DATA_TYPE = 0;

    public static final int QUERY_ID = 1;

    public static final int ROW_HEAP = 2;
    public static final int ROW_JOIN = 3;
    public static final int ROW_BATCH_LIST = 4;
    public static final int ROW_BATCH_EMPTY = 5;

    public static final int OPERATION_EXECUTE = 6;
    public static final int OPERATION_EXECUTE_FRAGMENT = 7;
    public static final int OPERATION_BATCH = 8;
    public static final int OPERATION_FLOW_CONTROL = 9;
    public static final int OPERATION_CANCEL = 10;
    public static final int OPERATION_CHECK = 11;
    public static final int OPERATION_CHECK_RESPONSE = 12;

    public static final int NODE_ROOT = 13;
    public static final int NODE_ROOT_SEND = 14;
    public static final int NODE_RECEIVE = 15;
    public static final int NODE_PROJECT = 16;
    public static final int NODE_FILTER = 17;
    public static final int NODE_MAP_SCAN = 18;

    public static final int EXPRESSION_COLUMN = 19;
    public static final int EXPRESSION_IS_NULL = 20;

    public static final int TARGET_DESCRIPTOR_GENERIC = 21;

    public static final int QUERY_PATH = 22;

    public static final int LEN = QUERY_PATH + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[QUERY_DATA_TYPE] = arg -> new QueryDataType();

        constructors[QUERY_ID] = arg -> new QueryId();

        constructors[ROW_HEAP] = arg -> new HeapRow();
        constructors[ROW_JOIN] = arg -> new JoinRow();
        constructors[ROW_BATCH_LIST] = arg -> new ListRowBatch();
        constructors[ROW_BATCH_EMPTY] = arg -> EmptyRowBatch.INSTANCE;

        constructors[OPERATION_EXECUTE] = arg -> new QueryExecuteOperation();
        constructors[OPERATION_EXECUTE_FRAGMENT] = arg -> new QueryExecuteOperationFragment();
        constructors[OPERATION_BATCH] = arg -> new QueryBatchExchangeOperation();
        constructors[OPERATION_FLOW_CONTROL] = arg -> new QueryFlowControlExchangeOperation();
        constructors[OPERATION_CANCEL] = arg -> new QueryCancelOperation();
        constructors[OPERATION_CHECK] = arg -> new QueryCheckOperation();
        constructors[OPERATION_CHECK_RESPONSE] = arg -> new QueryCheckResponseOperation();

        constructors[NODE_ROOT] = arg -> new RootPlanNode();
        constructors[NODE_ROOT_SEND] = arg -> new RootSendPlanNode();
        constructors[NODE_RECEIVE] = arg -> new ReceivePlanNode();
        constructors[NODE_PROJECT] = arg -> new ProjectPlanNode();
        constructors[NODE_FILTER] = arg -> new FilterPlanNode();
        constructors[NODE_MAP_SCAN] = arg -> new MapScanPlanNode();

        constructors[EXPRESSION_COLUMN] = arg -> new ColumnExpression<>();
        constructors[EXPRESSION_IS_NULL] = arg -> new IsNullPredicate();

        constructors[TARGET_DESCRIPTOR_GENERIC] = arg -> GenericQueryTargetDescriptor.INSTANCE;

        constructors[QUERY_PATH] = arg -> new QueryPath();

        return new ArrayDataSerializableFactory(constructors);
    }
}

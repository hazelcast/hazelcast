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
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ParameterExpression;
import com.hazelcast.sql.impl.expression.math.DivideFunction;
import com.hazelcast.sql.impl.expression.math.MinusFunction;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.expression.math.PlusFunction;
import com.hazelcast.sql.impl.expression.math.UnaryMinusFunction;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.CaseExpression;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.NotPredicate;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;
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
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.ProjectPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.RootSendPlanNode;
import com.hazelcast.sql.impl.row.EmptyRow;
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
    public static final int ROW_EMPTY = 4;
    public static final int ROW_BATCH_LIST = 5;
    public static final int ROW_BATCH_EMPTY = 6;

    public static final int OPERATION_EXECUTE = 7;
    public static final int OPERATION_EXECUTE_FRAGMENT = 8;
    public static final int OPERATION_BATCH = 9;
    public static final int OPERATION_FLOW_CONTROL = 10;
    public static final int OPERATION_CANCEL = 11;
    public static final int OPERATION_CHECK = 12;
    public static final int OPERATION_CHECK_RESPONSE = 13;

    public static final int NODE_ROOT = 14;
    public static final int NODE_ROOT_SEND = 15;
    public static final int NODE_RECEIVE = 16;
    public static final int NODE_PROJECT = 17;
    public static final int NODE_FILTER = 18;
    public static final int NODE_MAP_SCAN = 19;

    public static final int EXPRESSION_COLUMN = 20;
    public static final int EXPRESSION_IS_NULL = 21;

    public static final int TARGET_DESCRIPTOR_GENERIC = 22;

    public static final int QUERY_PATH = 23;

    public static final int EXPRESSION_CONSTANT = 24;
    public static final int EXPRESSION_PARAMETER = 25;
    public static final int EXPRESSION_CAST = 26;
    public static final int EXPRESSION_DIVIDE = 27;
    public static final int EXPRESSION_MINUS = 28;
    public static final int EXPRESSION_MULTIPLY = 29;
    public static final int EXPRESSION_PLUS = 30;
    public static final int EXPRESSION_UNARY_MINUS = 31;
    public static final int EXPRESSION_AND = 32;
    public static final int EXPRESSION_OR = 33;
    public static final int EXPRESSION_NOT = 34;
    public static final int EXPRESSION_COMPARISON = 35;
    public static final int EXPRESSION_CASE = 36;
    public static final int EXPRESSION_IS_TRUE = 37;
    public static final int EXPRESSION_IS_NOT_TRUE = 38;
    public static final int EXPRESSION_IS_FALSE = 39;
    public static final int EXPRESSION_IS_NOT_FALSE = 40;
    public static final int EXPRESSION_IS_NOT_NULL = 41;

    public static final int LEN = EXPRESSION_IS_NOT_NULL + 1;

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
        constructors[ROW_EMPTY] = arg -> EmptyRow.INSTANCE;
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

        constructors[EXPRESSION_CONSTANT] = arg -> new ConstantExpression<>();
        constructors[EXPRESSION_PARAMETER] = arg -> new ParameterExpression<>();
        constructors[EXPRESSION_CAST] = arg -> new CastExpression<>();
        constructors[EXPRESSION_DIVIDE] = arg -> new DivideFunction<>();
        constructors[EXPRESSION_MINUS] = arg -> new MinusFunction<>();
        constructors[EXPRESSION_MULTIPLY] = arg -> new MultiplyFunction<>();
        constructors[EXPRESSION_PLUS] = arg -> new PlusFunction<>();
        constructors[EXPRESSION_UNARY_MINUS] = arg -> new UnaryMinusFunction<>();
        constructors[EXPRESSION_AND] = arg -> new AndPredicate();
        constructors[EXPRESSION_OR] = arg -> new OrPredicate();
        constructors[EXPRESSION_NOT] = arg -> new NotPredicate();
        constructors[EXPRESSION_COMPARISON] = arg -> new ComparisonPredicate();
        constructors[EXPRESSION_CASE] = arg -> new CaseExpression<>();
        constructors[EXPRESSION_IS_TRUE] = arg -> new IsTruePredicate();
        constructors[EXPRESSION_IS_NOT_TRUE] = arg -> new IsNotTruePredicate();
        constructors[EXPRESSION_IS_FALSE] = arg -> new IsFalsePredicate();
        constructors[EXPRESSION_IS_NOT_FALSE] = arg -> new IsNotFalsePredicate();
        constructors[EXPRESSION_IS_NOT_NULL] = arg -> new IsNotNullPredicate();

        return new ArrayDataSerializableFactory(constructors);
    }
}

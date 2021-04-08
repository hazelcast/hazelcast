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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;

public class JoinByEquiJoinProcessorTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    @SuppressWarnings("unchecked")
    private static final Expression<Boolean> TRUE_PREDICATE =
            (Expression<Boolean>) ConstantExpression.create(true, BOOLEAN);
    private static final Expression<?> PROJECTION = ColumnExpression.create(1, VARCHAR);
    @SuppressWarnings("unchecked")
    private static final Expression<Boolean> FALSE_PREDICATE =
            (Expression<Boolean>) ConstantExpression.create(false, BOOLEAN);

    private IMap<Object, Object> map;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Before
    public void before() {
        map = instance().getMap(MAP_NAME);
    }

    @Test
    public void test_innerJoin() {
        map.put(1, "value");
        runTest(INNER, TRUE_PREDICATE, PROJECTION, TRUE_PREDICATE,
                singletonList(new Object[]{1}),
                singletonList(new Object[]{1, "value"}));
    }

    @Test
    public void when_innerJoinFilteredOutByProjector_then_absent() {
        map.put(1, "value");
        runTest(INNER, FALSE_PREDICATE, PROJECTION, TRUE_PREDICATE,
                singletonList(new Object[]{1}),
                emptyList());
    }

    @Test
    public void when_innerJoinProjectedByProjector_then_modified() {
        map.put(1, "value");
        runTest(INNER, TRUE_PREDICATE, ConstantExpression.create("modified", VARCHAR), TRUE_PREDICATE,
                singletonList(new Object[]{1}),
                singletonList(new Object[]{1, "modified"}));
    }

    @Test
    public void when_innerJoinFilteredOutByCondition_then_absent() {
        map.put(1, "value");
        runTest(INNER, TRUE_PREDICATE, PROJECTION, FALSE_PREDICATE,
                singletonList(new Object[]{1}),
                emptyList());
    }

    @Test
    public void test_outerJoin() {
        map.put(1, "value");
        runTest(LEFT, TRUE_PREDICATE, PROJECTION, TRUE_PREDICATE,
                asList(new Object[]{1}, new Object[]{2}),
                asList(new Object[]{1, "value"}, new Object[]{2, null}));
    }

    @Test
    public void when_outerJoinFilteredOutByProjector_then_absent() {
        map.put(1, "value");
        runTest(LEFT, FALSE_PREDICATE, PROJECTION, TRUE_PREDICATE,
                asList(new Object[]{1}, new Object[]{2}),
                asList(new Object[]{1, null}, new Object[]{2, null}));
    }

    @Test
    public void when_outerJoinProjectedByProjector_then_modified() {
        map.put(1, "value");
        runTest(LEFT, TRUE_PREDICATE, ConstantExpression.create("modified", VARCHAR), TRUE_PREDICATE,
                asList(new Object[]{1}, new Object[]{2}),
                asList(new Object[]{1, "modified"}, new Object[]{2, null}));
    }

    @Test
    public void when_outerJoinFilteredOutByCondition_then_absent() {
        map.put(1, "value");
        runTest(LEFT, TRUE_PREDICATE, PROJECTION, FALSE_PREDICATE,
                asList(new Object[]{1}, new Object[]{2}),
                asList(new Object[]{1, null}, new Object[]{2, null}));
    }

    private void runTest(
            JoinRelType joinType,
            Expression<Boolean> rowProjectorCondition,
            Expression<?> rowProjectorProjection,
            Expression<Boolean> nonEquiCondition,
            List<Object[]> input,
            List<Object[]> output
    ) {
        KvRowProjector.Supplier projectorSupplier = KvRowProjector.supplier(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, VARCHAR},
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                rowProjectorCondition,
                singletonList(rowProjectorProjection)
        );

        ProcessorMetaSupplier processor = JoinByEquiJoinProcessorSupplier.supplier(
                new JetJoinInfo(joinType, new int[]{0}, new int[]{0}, nonEquiCondition, null),
                MAP_NAME,
                projectorSupplier
        );

        TestSupport
                .verifyProcessor(adaptSupplier(processor))
                .input(input)
                .jetInstance(instance())
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableProgressAssertion()
                .expectOutput(output);
    }
}

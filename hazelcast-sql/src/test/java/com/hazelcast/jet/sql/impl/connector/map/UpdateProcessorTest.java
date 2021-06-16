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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ParameterExpression;
import com.hazelcast.sql.impl.expression.math.PlusFunction;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class UpdateProcessorTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

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
    public void test_update() {
        List<Expression<?>> updates = asList(
                ColumnExpression.create(0, INT),
                PlusFunction.create(ColumnExpression.create(1, INT), ConstantExpression.create(1, INT), INT)
        );

        executeUpdate(updates, singletonList(new Object[]{1}), emptyList());

        assertThat(map.get(1)).isEqualTo(2);
    }

    @Test
    public void test_updateWithDynamicParameter() {
        List<Expression<?>> updates = asList(
                ColumnExpression.create(0, INT),
                PlusFunction.create(ColumnExpression.create(1, INT), ParameterExpression.create(0, INT), INT)
        );

        executeUpdate(updates, singletonList(new Object[]{1}), singletonList(2));

        assertThat(map.get(1)).isEqualTo(3);
    }

    private void executeUpdate(
            List<Expression<?>> updates,
            List<Object[]> input,
            List<Object> arguments
    ) {
        QueryPath[] paths = new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH};
        QueryDataType[] types = new QueryDataType[]{INT, INT};
        KvRowProjector.Supplier rowProjectorSupplier = KvRowProjector.supplier(
                paths,
                types,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                null,
                asList(ColumnExpression.create(0, INT), ColumnExpression.create(1, INT))
        );
        KvProjector.Supplier projectorSupplier = KvProjector.supplier(
                paths,
                types,
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                PrimitiveUpsertTargetDescriptor.INSTANCE
        );

        UpdateProcessorSupplier processor = new UpdateProcessorSupplier(
                MAP_NAME, rowProjectorSupplier, updates, projectorSupplier
        );

        TestSupport
                .verifyProcessor(adaptSupplier(processor))
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, arguments))
                .executeBeforeEachRun(() -> map.put(1, 1))
                .input(input)
                .hazelcastInstance(instance())
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableProgressAssertion()
                .expectOutput(emptyList());
    }
}

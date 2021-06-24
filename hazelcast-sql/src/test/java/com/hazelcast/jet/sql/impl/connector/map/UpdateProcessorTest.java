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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
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
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class UpdateProcessorTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private IMap<Integer, Object> map;

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
        Object updated = executeUpdate(
                INT,
                1,
                PlusFunction.create(ColumnExpression.create(1, INT), ConstantExpression.create(1, INT), INT),
                emptyList()
        );
        assertThat(updated).isEqualTo(2);
    }

    @Test
    public void test_updateWithDynamicParameter() {
        Object updated = executeUpdate(
                BIGINT,
                2L,
                PlusFunction.create(ColumnExpression.create(1, BIGINT), ParameterExpression.create(0, BIGINT), BIGINT),
                singletonList(2L)
        );
        assertThat(updated).isEqualTo(4L);
    }

    @SuppressWarnings("SameParameterValue")
    private Object executeUpdate(
            QueryDataType type,
            Object value,
            Expression<?> update,
            List<Object> arguments
    ) {
        KvRowProjector.Supplier rowProjectorSupplier = KvRowProjector.supplier(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{INT, type},
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                null,
                asList(ColumnExpression.create(0, INT), ColumnExpression.create(1, type))
        );

        ValueProjector.Supplier valueProjectorSupplier = ValueProjector.supplier(
                new QueryPath[]{QueryPath.VALUE_PATH},
                new QueryDataType[]{type},
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                singletonList(update)
        );

        UpdateProcessorSupplier processor = new UpdateProcessorSupplier(
                MAP_NAME, rowProjectorSupplier, valueProjectorSupplier
        );

        TestSupport
                .verifyProcessor(adaptSupplier(processor))
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, arguments))
                .executeBeforeEachRun(() -> map.put(1, value))
                .input(singletonList(new Object[]{1}))
                .hazelcastInstance(instance())
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableProgressAssertion()
                .expectOutput(emptyList());

        return map.get(1);
    }
}

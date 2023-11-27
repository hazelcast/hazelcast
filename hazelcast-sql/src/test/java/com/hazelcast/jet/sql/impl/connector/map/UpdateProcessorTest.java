/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.ParameterExpression;
import com.hazelcast.sql.impl.expression.math.PlusFunction;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.Subject;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UpdateProcessorTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private Map<Integer, Object> map;

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
                1,
                1,
                partitionedTable(INT),
                singletonList(VALUE),
                singletonList(PlusFunction.create(ColumnExpression.create(1, INT), ConstantExpression.create(1, INT), INT)),
                emptyList()
        );
        assertThat(updated).isEqualTo(2);
    }

    @Test
    public void test_updateWithDynamicParameter() {
        Object updated = executeUpdate(
                2L,
                1,
                partitionedTable(BIGINT),
                singletonList(VALUE),
                singletonList(PlusFunction.create(ColumnExpression.create(1, BIGINT), ParameterExpression.create(0, BIGINT), BIGINT)),
                singletonList(2L)
        );
        assertThat(updated).isEqualTo(4L);
    }

    @Test
    public void when_keyDoesNotExist_then_doesNotCreateEntry() {
        Object updated = executeUpdate(
                1,
                0,
                partitionedTable(INT),
                singletonList(VALUE),
                singletonList(PlusFunction.create(ColumnExpression.create(1, INT), ConstantExpression.create(1, INT), INT)),
                emptyList()
        );
        assertThat(updated).isEqualTo(1);
        assertThat(map).containsExactly(entry(1, 1));
    }

    @Test
    public void when_serializedObject_then_deserializedCorrect() {
        AbstractSerializationService service = (AbstractSerializationService) TestUtil.getNode(instance()).getSerializationService();

        var evalContextMock = mock(ExpressionEvalContext.class);
        when(evalContextMock.getSerializationService()).thenReturn(mock());
        var subject = new Subject(true, emptySet(), emptySet(), emptySet());
        when(evalContextMock.subject()).thenReturn(subject);

        var processor = UpdatingEntryProcessor.supplier(
                partitionedTable(INT),
                emptyList(),
                emptyList()
        ).get(evalContextMock);

        var data = service.toData(processor);
        var actual = service.toObject(data);

        assertThat(actual)
                .usingRecursiveComparison()
                .comparingOnlyFields("rowProjectorSupplier", "valueProjectorSupplier", "arguments", "subject")
                .ignoringFields("subject.pubCredentials", "subject.privCredentials")
                .isEqualTo(processor);
    }

    private static PartitionedMapTable partitionedTable(QueryDataType valueType) {
        return new PartitionedMapTable(
                QueryUtils.SCHEMA_NAME_PUBLIC,
                MAP_NAME,
                MAP_NAME,
                asList(
                        new MapTableField(KEY, INT, false, QueryPath.KEY_PATH),
                        new MapTableField(VALUE, valueType, false, QueryPath.VALUE_PATH)
                ),
                new ConstantTableStatistics(1),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                emptyList(),
                false,
                Collections.emptyList(),
                false);
    }

    private Object executeUpdate(
            Object initialValue,
            int inputValue,
            PartitionedMapTable table,
            List<String> fieldNames,
            List<Expression<?>> expressions,
            List<Object> arguments
    ) {
        UpdateProcessorSupplier processor = new UpdateProcessorSupplier(
                MAP_NAME, UpdatingEntryProcessor.supplier(table, fieldNames, expressions)
        );

        TestSupport
                .verifyProcessor(adaptSupplier(processor))
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, arguments))
                .executeBeforeEachRun(() -> map.put(1, initialValue))
                .input(singletonList(jetRow(inputValue)))
                .hazelcastInstance(instance())
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableProgressAssertion()
                .expectOutput(emptyList());

        return map.get(1);
    }
}

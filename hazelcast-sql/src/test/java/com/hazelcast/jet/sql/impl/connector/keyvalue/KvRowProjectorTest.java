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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.math.DivideFunction;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.extract.GenericQueryTarget;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.sql.SqlTestSupport.jetRow;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KvRowProjectorTest {

    @Test
    public void test_project() {
        KvRowProjector projector = new KvRowProjector(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{INT, INT},
                new IdentityTarget(),
                new IdentityTarget(),
                null,
                asList(
                        MultiplyFunction.create(ColumnExpression.create(0, INT), ConstantExpression.create(2, INT), INT),
                        DivideFunction.create(ColumnExpression.create(1, INT), ConstantExpression.create(2, INT), INT)
                ),
                mock(ExpressionEvalContext.class)
        );

        JetSqlRow row = projector.project(1, 8);

        assertThat(row.getValues()).isEqualTo(new Object[]{2, 4});
    }

    @Test
    public void test_project_onlyDataKeyAndValueIsProvided() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        KvRowProjector projector = new KvRowProjector(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{INT, INT},
                new GenericQueryTarget(serializationService, null, true),
                new GenericQueryTarget(serializationService, null, false),
                null,
                asList(
                        MultiplyFunction.create(ColumnExpression.create(0, INT), ConstantExpression.create(2, INT), INT),
                        DivideFunction.create(ColumnExpression.create(1, INT), ConstantExpression.create(2, INT), INT)
                ),
                SqlTestSupport.createExpressionEvalContext()
        );

        JetSqlRow row = projector.project(serializationService.toData(1), serializationService.toData(8));

        assertThat(row).isEqualTo(jetRow(2, 4));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void when_filteredByPredicate_then_returnsNull() {
        KvRowProjector projector = new KvRowProjector(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{INT, INT},
                new IdentityTarget(),
                new IdentityTarget(),
                (Expression<Boolean>) ConstantExpression.create(Boolean.FALSE, BOOLEAN),
                emptyList(),
                mock(ExpressionEvalContext.class)
        );

        JetSqlRow row = projector.project(1, 8);

        assertThat(row).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_supplierSerialization() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        KvRowProjector.Supplier original = KvRowProjector.supplier(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.INT},
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                (Expression<Boolean>) ConstantExpression.create(Boolean.FALSE, QueryDataType.BOOLEAN),
                asList(ConstantExpression.create(1, QueryDataType.INT), ConstantExpression.create("2", QueryDataType.INT))
        );

        KvRowProjector.Supplier serialized = serializationService.toObject(serializationService.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }

    private static final class IdentityTarget implements QueryTarget {

        private Object value;

        private IdentityTarget() {
        }

        @Override
        public void setTarget(Object value, Data valueData) {
            this.value = value;
        }

        @Override
        public QueryExtractor createExtractor(String path, QueryDataType type) {
            return () -> value;
        }
    }
}

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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.Field;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetTestSupport;
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.JetTestSupport.TEST_SS;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProjectorTest extends UpsertTargetTestSupport {

    @Test
    public void test_project() {
        Projector projector = new Projector(
                List.of(field(VALUE, BIGINT)),
                new MultiplyingTarget(),
                List.of(CastExpression.create(ColumnExpression.create(0, INT), BIGINT)),
                mock(ExpressionEvalContext.class)
        );

        Object value = projector.project(new JetSqlRow(TEST_SS, new Object[]{1}));

        assertThat(value).isEqualTo(2L);
    }

    @Test
    public void test_supplierSerialization() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        Projector.Supplier original = Projector.supplier(
                List.of(field("field", INT)),
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                List.of(ColumnExpression.create(0, INT))
        );

        Projector.Supplier serialized = serializationService.toObject(serializationService.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }

    private static final class MultiplyingTarget extends UpsertTarget {
        @Override
        protected Converter<Long> createConverter(Stream<Field> fields) {
            return value -> (long) value * 2;
        }
    }
}

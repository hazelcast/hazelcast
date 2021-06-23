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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.UpsertInjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ValueProjectorTest {

    @Test
    public void test_project() {
        ValueProjector projector = new ValueProjector(
                new QueryPath[]{QueryPath.create("this.field")},
                new QueryDataType[]{QueryDataType.BIGINT},
                new MultiplyingTarget(),
                singletonList(CastExpression.create(ColumnExpression.create(0, QueryDataType.INT), QueryDataType.BIGINT)),
                mock(ExpressionEvalContext.class)
        );

        Object value = projector.project(new Object[]{1});

        assertThat(value).isEqualTo(2L);
    }

    @Test
    public void test_supplierSerialization() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        ValueProjector.Supplier original = ValueProjector.supplier(
                new QueryPath[]{QueryPath.create("this.field")},
                new QueryDataType[]{QueryDataType.INT},
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                singletonList(ColumnExpression.create(0, QueryDataType.INT))
        );

        ValueProjector.Supplier serialized = serializationService.toObject(serializationService.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }

    private static final class MultiplyingTarget implements UpsertTarget {

        private Object value;

        private MultiplyingTarget() {
            value = -1;
        }

        @Override
        public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
            return value -> this.value = value;
        }

        @Override
        public void init() {
            value = null;
        }

        @Override
        public Object conclude() {
            return (long) value * 2;
        }
    }
}

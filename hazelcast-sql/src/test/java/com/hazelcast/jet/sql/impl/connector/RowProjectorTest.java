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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RowProjectorTest {

    @Test
    public void test_project() {
        RowProjector projector = new RowProjector(
                new String[]{"target"},
                new QueryDataType[]{INT},
                new IdentityTarget(),
                null,
                singletonList(
                        MultiplyFunction.create(ColumnExpression.create(0, INT), ConstantExpression.create(2, INT), INT)
                ),
                mock(ExpressionEvalContext.class)
        );

        JetSqlRow row = projector.project(1);

        assertThat(row.getValues()).isEqualTo(new Object[]{2});
    }

    @Test
    @SuppressWarnings("unchecked")
    public void when_filteredByPredicate_then_returnsNull() {
        RowProjector projector = new RowProjector(
                new String[]{"target"},
                new QueryDataType[]{INT},
                new IdentityTarget(),
                (Expression<Boolean>) ConstantExpression.create(Boolean.FALSE, BOOLEAN),
                emptyList(),
                mock(ExpressionEvalContext.class)
        );

        JetSqlRow row = projector.project(1);

        assertThat(row).isNull();
    }

    private static final class IdentityTarget implements QueryTarget {

        private Object value;

        private IdentityTarget() {
        }

        @Override
        public void setTarget(Object value, Data valueData) {
            assert valueData == null;
            this.value = value;
        }

        @Override
        public QueryExtractor createExtractor(String path, QueryDataType type) {
            return () -> value;
        }
    }
}

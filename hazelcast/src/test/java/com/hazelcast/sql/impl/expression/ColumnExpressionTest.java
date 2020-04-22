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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static groovy.util.GroovyTestCase.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ColumnExpressionTest extends SqlTestSupport {
    @Test
    public void testColumnExpression() {
        int index = 1;

        ColumnExpression<?> expression = ColumnExpression.create(index, QueryDataType.INT);

        assertEquals(QueryDataType.INT, expression.getType());

        HeapRow row = HeapRow.of(new Object(), new Object(), new Object());
        assertSame(row.get(index), expression.eval(row, SimpleExpressionEvalContext.create()));
    }

    @Test
    public void testEquality() {
        checkEquals(ColumnExpression.create(1, QueryDataType.INT), ColumnExpression.create(1, QueryDataType.INT), true);
        checkEquals(ColumnExpression.create(1, QueryDataType.INT), ColumnExpression.create(1, QueryDataType.BIGINT), false);
        checkEquals(ColumnExpression.create(1, QueryDataType.INT), ColumnExpression.create(2, QueryDataType.INT), false);
    }

    @Test
    public void testSerialization() {
        ColumnExpression<?> original = ColumnExpression.create(1, QueryDataType.INT);
        ColumnExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_COLUMN);

        checkEquals(original, restored, true);
    }
}

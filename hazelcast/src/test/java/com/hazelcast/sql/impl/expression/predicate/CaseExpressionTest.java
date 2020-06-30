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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.SimpleExpressionEvalContext;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CaseExpressionTest extends SqlTestSupport {

    // NOTE: This test class verifies only basic functionality, look for more
    // extensive tests in hazelcast-sql module.

    @Test
    public void testCreationAndEval() {
        assertEquals(0, case_(INT, true, 0, false, 1, 2).eval(row("foo"), SimpleExpressionEvalContext.create()));
        assertEquals(1, case_(INT, false, 0, true, 1, 2).eval(row("foo"), SimpleExpressionEvalContext.create()));
        assertEquals(2, case_(INT, false, 0, false, 1, 2).eval(row("foo"), SimpleExpressionEvalContext.create()));
    }

    @Test
    public void testEquality() {
        checkEquals(case_(INT, true, 0, false, 1, 2), case_(INT, true, 0, false, 1, 2), true);
        checkEquals(case_(INT, true, 0, false, 1, 2), case_(BIGINT, true, 0, false, 1, 2), false);
        checkEquals(case_(INT, true, 0, false, 1, 2), case_(INT, true, 0, false, 1, 3), false);
        checkEquals(case_(INT, true, 0, false, 1, 2), case_(INT, false, 0, true, 1, 2), false);
    }

    @Test
    public void testSerialization() {
        CaseExpression<?> original = case_(INT, true, 0, false, 1, 2);
        CaseExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_CASE);

        checkEquals(original, restored, true);
    }

    private static CaseExpression<?> case_(QueryDataType type, Object... values) {
        Expression<?>[] operands = new Expression<?>[values.length];
        for (int i = 0; i < values.length; ++i) {
            Object value = values[i];
            operands[i] = ConstantExpression.create(value, value instanceof Boolean ? BOOLEAN : INT);
        }
        return CaseExpression.create(operands, type);
    }

}

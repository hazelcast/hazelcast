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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ParameterTest extends ExpressionTestBase {

    @Test
    public void verify() {
        verify(IDENTITY, ParameterTest::expectedTypes, ParameterTest::expectedValues, "%s", PARAMETERS);
    }

    @Test
    public void testCreationAndEval() {
        ParameterExpression<?> expression = ParameterExpression.create(1, INT);
        assertEquals(INT, expression.getType());
        assertEquals("baz", expression.eval(row("foo"), SimpleExpressionEvalContext.create("bar", "baz")));
    }

    @Test
    public void testEquality() {
        checkEquals(ParameterExpression.create(1, INT), ParameterExpression.create(1, INT), true);
        checkEquals(ParameterExpression.create(1, INT), ParameterExpression.create(1, BIGINT), false);
        checkEquals(ParameterExpression.create(1, INT), ParameterExpression.create(2, INT), false);
    }

    @Test
    public void testSerialization() {
        ParameterExpression<?> original = ParameterExpression.create(1, INT);
        ParameterExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_PARAMETER);

        checkEquals(original, restored, true);
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        return null;
    }

    private static Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        return INVALID_VALUE;
    }

}

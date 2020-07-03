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

import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LiteralTest extends ExpressionTestBase {

    @Test
    public void testEndToEnd() {
        SqlService sql = createEndToEndRecords();
        assertRows(query(sql, "select __key, __key + 1 + '1.1' from records"), keyRange(0, 1000, 5000, 6000), k -> k + 2.1);
        assertRows(query(sql, "select __key, null from records"), keyRange(0, 1000, 5000, 6000), k -> null);
        assertQueryThrows(sql, "select 1 + 'not a number' from records", "can not be parsed");
    }

    @Test
    public void verify() {
        verify(IDENTITY, LiteralTest::expectedTypes, LiteralTest::expectedValues, "%s", LITERALS);
    }

    @Test
    public void testCreationAndEval() {
        ConstantExpression<?> expression = ConstantExpression.create(1, INT);
        assertEquals(INT, expression.getType());
        assertEquals(1, expression.eval(row("foo"), SimpleExpressionEvalContext.create()));
    }

    @Test
    public void testEquality() {
        checkEquals(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT), true);
        checkEquals(ConstantExpression.create(1, INT), ConstantExpression.create(1, BIGINT), false);
        checkEquals(ConstantExpression.create(1, INT), ConstantExpression.create(2, INT), false);
    }

    @Test
    public void testSerialization() {
        ConstantExpression<?> original = ConstantExpression.create(1, INT);
        ConstantExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_CONSTANT);

        checkEquals(original, restored, true);
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        Operand operand = operands[0];
        RelDataType type = operand.type;

        // Assign type to numeric literals.

        if (operand.isNumericLiteral()) {
            BigDecimal numeric = operand.numericValue();
            //noinspection NumberEquality
            assert numeric != null && numeric != INVALID_NUMERIC_VALUE;
            type = narrowestTypeFor(numeric, null);
        }

        // Validate literals.

        if (operand.isLiteral() && !canRepresentLiteral(operand, type)) {
            return null;
        }

        return new RelDataType[]{type, type};
    }

    private static Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        return args[0];
    }

}

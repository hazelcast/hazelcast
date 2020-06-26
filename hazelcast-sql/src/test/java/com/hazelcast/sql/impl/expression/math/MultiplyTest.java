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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastReturnTypes;
import com.hazelcast.sql.impl.expression.ExpressionTestBase;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.MULTIPLY;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType.canOverflow;
import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiplyTest extends ExpressionTestBase {

    @Test
    public void verify() {
        verify(MULTIPLY, MultiplyTest::expectedTypes, MultiplyTest::expectedValues, ALL, ALL);
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        // Infer types.

        RelDataType[] types = inferTypes(operands, true);
        if (types == null) {
            return null;
        }
        RelDataType commonType = types[2];

        // Validate, coerce and infer return type.

        if (!isNumeric(commonType)) {
            return null;
        }

        for (int i = 0; i < types.length - 1; ++i) {
            Operand operand = operands[i];
            RelDataType type = types[i];

            if (!isNumeric(type) && !isNull(type)) {
                return null;
            }

            if (operand.isLiteral() && !canRepresentLiteral(operand, type)) {
                return null;
            }
        }

        if (isInteger(commonType)) {
            types[2] = HazelcastReturnTypes.integerMultiply(types[0], types[1]);
        }

        return types;
    }

    private static Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        RelDataType type = types[2];
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == NULL) {
            return null;
        }

        Object lhs = args[0];
        Object rhs = args[1];

        if (operands[0].isLiteral() && lhs == null || operands[1].isLiteral() && rhs == null) {
            return null;
        }

        if (lhs == INVALID_VALUE) {
            return INVALID_VALUE;
        }
        if (lhs == null) {
            return null;
        }

        if (rhs == INVALID_VALUE) {
            return INVALID_VALUE;
        }
        if (rhs == null) {
            return null;
        }

        switch (typeName) {
            case TINYINT:
                long byteResult = Math.multiplyExact(number(lhs).longValue(), number(rhs).longValue());
                assertTrue(byteResult >= Byte.MIN_VALUE && byteResult <= Byte.MAX_VALUE);
                return (byte) byteResult;
            case SMALLINT:
                long shortResult = Math.multiplyExact(number(lhs).longValue(), number(rhs).longValue());
                assertTrue(shortResult >= Short.MIN_VALUE && shortResult <= Short.MAX_VALUE);
                return (short) shortResult;
            case INTEGER:
                long intResult = Math.multiplyExact(number(lhs).longValue(), number(rhs).longValue());
                assertTrue(intResult >= Integer.MIN_VALUE && intResult <= Integer.MAX_VALUE);
                return (int) intResult;
            case BIGINT:
                try {
                    return Math.multiplyExact(number(lhs).longValue(), number(rhs).longValue());
                } catch (ArithmeticException e) {
                    assertTrue(canOverflow(type));
                    return INVALID_VALUE;
                }
            case REAL:
                return number(lhs).floatValue() * number(rhs).floatValue();
            case DOUBLE:
                return number(lhs).doubleValue() * number(rhs).doubleValue();
            case DECIMAL:
                return ((BigDecimal) lhs).multiply((BigDecimal) rhs, DECIMAL_MATH_CONTEXT);

            default:
                throw new IllegalArgumentException("unexpected type name: " + typeName);
        }
    }

}

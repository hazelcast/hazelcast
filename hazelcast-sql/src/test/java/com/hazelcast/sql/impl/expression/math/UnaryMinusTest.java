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

import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.UNARY_MINUS;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType.canOverflow;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnaryMinusTest extends ExpressionTestBase {

    @Test
    public void verify() {
        verify(UNARY_MINUS, UnaryMinusTest::expectedTypes, UnaryMinusTest::expectedValues, ALL);
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        Operand operand = operands[0];

        if (operand.isParameter()) {
            return null;
        }

        if (operand.typeName() == NULL) {
            return null;
        }

        if (operand.typeName() == ANY) {
            return null;
        }

        if (operand.typeName() == BOOLEAN) {
            return null;
        }

        RelDataType type = operand.type;
        boolean isChar = isChar(type);

        BigDecimal numeric = operand.numericValue();
        //noinspection NumberEquality
        if (numeric == INVALID_NUMERIC_VALUE) {
            return null;
        }

        if (numeric != null) {
            if (!isChar) {
                numeric = numeric.negate();
            }
            type = narrowestTypeFor(numeric, null);
            if (!canRepresentLiteral(numeric, TYPE_FACTORY.createSqlType(DECIMAL), type)) {
                return null;
            }
        } else if (isChar) {
            type = TYPE_FACTORY.createSqlType(DOUBLE, type.isNullable());
        }

        RelDataType returnType = type;
        if (isInteger(returnType) && (!operand.isLiteral() || isChar)) {
            returnType = HazelcastReturnTypes.integerUnaryMinus(returnType);
        }

        return new RelDataType[]{type, returnType};
    }

    private static Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        Operand operand = operands[0];
        Object arg = args[0];
        RelDataType type = types[0];
        SqlTypeName typeName = typeName(type);
        RelDataType returnType = types[1];

        SqlTypeName returnTypeName = typeName(returnType);
        if (returnTypeName == NULL) {
            return null;
        }
        if (arg == null) {
            return null;
        }

        // XXX: Special case of SELECT -(9223372036854775808), Calcite interprets
        // this as a literal (Long.MIN_VALUE), but it's impossible to pass
        // abs(Long.MIN_VALUE) from Java side in a form of a long to negate it.
        if (arg == INVALID_VALUE && operand.isLiteral() && typeName == BIGINT) {
            BigDecimal numeric = operand.numericValue();
            assert numeric != null;
            //noinspection NumberEquality
            if (numeric != INVALID_NUMERIC_VALUE) {
                numeric = numeric.negate(DECIMAL_MATH_CONTEXT);
                if (numeric.longValueExact() == Long.MIN_VALUE) {
                    return Long.MIN_VALUE;
                }
            }
        }

        if (arg == INVALID_VALUE) {
            return INVALID_VALUE;
        }

        switch (returnTypeName) {
            case TINYINT:
                long byteResult = -number(arg).longValue();
                assertTrue(byteResult >= Byte.MIN_VALUE && byteResult <= Byte.MAX_VALUE);
                return (byte) byteResult;
            case SMALLINT:
                long shortResult = -number(arg).longValue();
                assertTrue(shortResult >= Short.MIN_VALUE && shortResult <= Short.MAX_VALUE);
                return (short) shortResult;
            case INTEGER:
                long intResult = -number(arg).longValue();
                assertTrue(intResult >= Integer.MIN_VALUE && intResult <= Integer.MAX_VALUE);
                return (int) intResult;
            case BIGINT:
                try {
                    return Math.negateExact(number(arg).longValue());
                } catch (ArithmeticException e) {
                    assertTrue(canOverflow(returnType));
                    return INVALID_VALUE;
                }
            case REAL:
                float floatResult = -number(arg).floatValue();
                // XXX: -(0.0) interpreted by Calcite as a positive zero, but -('0.0') is not
                if (operand.isLiteral() && !isChar(operand.type) && Float.compare(floatResult, -0.0F) == 0) {
                    floatResult = 0.0F;
                }
                return floatResult;
            case DOUBLE:
                double doubleResult = -number(arg).doubleValue();
                // XXX: -(0.0) interpreted by Calcite as a positive zero, but -('0.0') is not
                if (operand.isLiteral() && !isChar(operand.type) && Double.compare(doubleResult, -0.0D) == 0) {
                    doubleResult = 0.0D;
                }
                return doubleResult;
            case DECIMAL:
                return ((BigDecimal) arg).negate(DECIMAL_MATH_CONTEXT);
            default:
                throw new IllegalArgumentException("unexpected type name: " + returnTypeName);
        }
    }

}

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
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionTestBase;
import com.hazelcast.sql.impl.expression.SimpleExpressionEvalContext;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.AND;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AndTest extends ExpressionTestBase {

    @Test
    public void verify() {
        verify(AND, AndTest::expectedTypes, AndTest::expectedValues, ALL, ALL);
    }

    @Test
    public void testCreationAndEval() {
        assertFalse(and(true, false).eval(row("foo"), SimpleExpressionEvalContext.create()));
        assertTrue(and(true, true).eval(row("foo"), SimpleExpressionEvalContext.create()));
        assertNull(and(null, true).eval(row("foo"), SimpleExpressionEvalContext.create()));
    }

    @Test
    public void testEquality() {
        checkEquals(and(true, false), and(true, false), true);
        checkEquals(and(true, false), and(true, true), false);
        checkEquals(and(true, true), and(true, true, true), false);
    }

    @Test
    public void testSerialization() {
        AndPredicate original = and(true, false);
        AndPredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_AND);

        checkEquals(original, restored, true);
    }

    private static AndPredicate and(Boolean... values) {
        Expression<?>[] operands = new Expression<?>[values.length];
        for (int i = 0; i < values.length; ++i) {
            operands[i] = ConstantExpression.create(values[i], QueryDataType.BOOLEAN);
        }
        return AndPredicate.create(operands);
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        RelDataType[] types = new RelDataType[operands.length + 1];

        boolean seenNullable = false;
        for (int i = 0; i < operands.length; ++i) {
            Operand operand = operands[i];

            if (operand.isParameter()) {
                types[i] = TYPE_FACTORY.createSqlType(BOOLEAN, true);
                seenNullable = true;
            } else {
                assert operand.type != UNKNOWN_TYPE;

                switch (operand.typeName()) {
                    case NULL:
                        types[i] = TYPE_FACTORY.createSqlType(BOOLEAN, true);
                        seenNullable = true;
                        break;

                    case BOOLEAN:
                        types[i] = operand.type;
                        seenNullable |= operand.type.isNullable();
                        break;

                    case VARCHAR:
                        if (operand.isLiteral()) {
                            Boolean booleanValue = operand.booleanValue();
                            if (booleanValue == INVALID_BOOLEAN_VALUE) {
                                return null;
                            }
                        }

                        types[i] = TYPE_FACTORY.createSqlType(BOOLEAN, operand.type.isNullable());
                        seenNullable |= operand.type.isNullable();
                        break;

                    default:
                        return null;
                }
            }
        }

        types[types.length - 1] = TYPE_FACTORY.createSqlType(BOOLEAN, seenNullable);
        return types;
    }

    private static Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        for (int i = 0; i < args.length; ++i) {
            Operand operand = operands[i];
            Object arg = args[i];

            if (operand.isLiteral() && arg != INVALID_VALUE && TernaryLogic.isFalse((Boolean) arg)) {
                return false;
            }
        }

        boolean seenUnknown = false;

        for (Object arg : args) {
            if (arg == INVALID_VALUE) {
                return INVALID_VALUE;
            }

            if (TernaryLogic.isFalse((Boolean) arg)) {
                return false;
            }

            if (arg == null) {
                seenUnknown = true;
            }
        }

        return seenUnknown ? null : true;
    }

}

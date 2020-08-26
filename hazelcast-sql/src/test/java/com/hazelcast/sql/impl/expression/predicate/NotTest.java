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

import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.NOT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NotTest extends ExpressionTestBase {

    @Test
    public void verify() {
        verify(NOT, NotTest::expectedTypes, NotTest::expectedValues, "NOT %s", ALL);
    }

    @Test
    public void testCreationAndEval() {
        assertFalse(not(true).eval(row("foo"), SimpleExpressionEvalContext.create()));
        assertTrue(not(false).eval(row("foo"), SimpleExpressionEvalContext.create()));
    }

    @Test
    public void testEquality() {
        checkEquals(not(true), not(true), true);
        checkEquals(not(true), not(false), false);
    }

    @Test
    public void testSerialization() {
        NotPredicate original = not(true);
        NotPredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_NOT);

        checkEquals(original, restored, true);
    }

    private static NotPredicate not(Boolean value) {
        return NotPredicate.create(ConstantExpression.create(value, QueryDataType.BOOLEAN));
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        Operand operand = operands[0];
        RelDataType type = operand.type;

        if (operand.isParameter()) {
            type = TYPE_FACTORY.createSqlType(BOOLEAN, true);
        } else {
            assert operand.type != UNKNOWN_TYPE;

            switch (operand.typeName()) {
                case NULL:
                    type = TYPE_FACTORY.createSqlType(BOOLEAN, true);
                    break;

                case BOOLEAN:
                    // do nothing
                    break;

                case VARCHAR:
                    if (operand.isLiteral()) {
                        Boolean booleanValue = operand.booleanValue();
                        if (booleanValue == INVALID_BOOLEAN_VALUE) {
                            return null;
                        }
                    }

                    type = TYPE_FACTORY.createSqlType(BOOLEAN, operand.type.isNullable());
                    break;

                default:
                    return null;
            }
        }

        return new RelDataType[]{type, type};
    }

    private static Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        Object arg = args[0];

        if (arg == INVALID_VALUE) {
            return INVALID_VALUE;
        }

        return TernaryLogic.not((Boolean) arg);
    }

}

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
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastObjectType;
import com.hazelcast.sql.impl.expression.ColumnExpression;
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

import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.IS_NOT_NULL;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IsNotNullTest extends ExpressionTestBase {

    @Test
    public void verify() {
        verify(IS_NOT_NULL, IsNotNullTest::expectedTypes, IsNotNullTest::expectedValues, "%s IS NOT NULL", ALL);
    }

    @Test
    public void testCreationAndEval() {
        IsNotNullPredicate predicate = IsNotNullPredicate.create(ColumnExpression.create(0, QueryDataType.VARCHAR));

        assertTrue(predicate.eval(row("test"), SimpleExpressionEvalContext.create()));
        assertFalse(predicate.eval(row(new Object[]{null}), SimpleExpressionEvalContext.create()));
    }

    @Test
    public void testEquality() {
        ColumnExpression<?> column1 = ColumnExpression.create(1, QueryDataType.VARCHAR);
        ColumnExpression<?> column2 = ColumnExpression.create(2, QueryDataType.VARCHAR);

        checkEquals(IsNotNullPredicate.create(column1), IsNotNullPredicate.create(column1), true);
        checkEquals(IsNotNullPredicate.create(column1), IsNotNullPredicate.create(column2), false);
    }

    @Test
    public void testSerialization() {
        IsNotNullPredicate original = IsNotNullPredicate.create(ColumnExpression.create(1, QueryDataType.VARCHAR));
        IsNotNullPredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_IS_NOT_NULL);

        checkEquals(original, restored, true);
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        Operand operand = operands[0];
        RelDataType type = operand.type;

        if (operand.isParameter()) {
            return new RelDataType[]{HazelcastObjectType.NULLABLE_INSTANCE, TYPE_FACTORY.createSqlType(BOOLEAN)};
        }

        // Assign type to numeric literals.

        if (operand.isNumericLiteral()) {
            Number numeric = operand.numericValue();
            assert numeric != null && numeric != INVALID_NUMERIC_VALUE;
            type = narrowestTypeFor(numeric, null);
        }

        // Validate literals.

        if (operand.isLiteral() && !canRepresentLiteral(operand, type)) {
            return null;
        }

        return new RelDataType[]{type, TYPE_FACTORY.createSqlType(BOOLEAN)};
    }

    private static Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        Object arg = args[0];

        if (arg == INVALID_VALUE) {
            return INVALID_VALUE;
        }

        return TernaryLogic.isNotNull(arg);
    }

}

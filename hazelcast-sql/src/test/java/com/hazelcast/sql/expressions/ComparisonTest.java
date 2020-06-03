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

package com.hazelcast.sql.expressions;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.canCast;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.withHigherPrecedence;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.EQUALS;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.GREATER_THAN;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.GREATER_THAN_OR_EQUAL;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.LESS_THAN;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.LESS_THAN_OR_EQUAL;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class ComparisonTest extends ExpressionTestBase {

    @Parameter
    public ComparisonMode mode;

    @Parameters(name = "mode:{0}")
    public static Collection<Object[]> parameters() {
        //@formatter:off
        return Arrays.asList(new Object[][]{
                {ComparisonMode.EQUALS},
                {ComparisonMode.NOT_EQUALS},
                {ComparisonMode.GREATER_THAN},
                {ComparisonMode.GREATER_THAN_OR_EQUAL},
                {ComparisonMode.LESS_THAN},
                {ComparisonMode.LESS_THAN_OR_EQUAL}
        });
        //@formatter:on
    }

    @Test
    public void verify() {
        SqlOperator operator;
        switch (mode) {
            case EQUALS:
                operator = EQUALS;
                break;
            case NOT_EQUALS:
                operator = NOT_EQUALS;
                break;
            case GREATER_THAN:
                operator = GREATER_THAN;
                break;
            case GREATER_THAN_OR_EQUAL:
                operator = GREATER_THAN_OR_EQUAL;
                break;
            case LESS_THAN:
                operator = LESS_THAN;
                break;
            case LESS_THAN_OR_EQUAL:
                operator = LESS_THAN_OR_EQUAL;
                break;
            default:
                throw new IllegalStateException("unexpected mode: " + mode);
        }

        verify(operator, ComparisonTest::expectedTypes, this::expectedValues, ALL, ALL);
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        Operand lhs = operands[0];
        Operand rhs = operands[1];

        // Trim some trivial cases.

        if (lhs.isParameter() && rhs.isParameter()) {
            return null;
        }

        RelDataType lhsType = lhs.type;
        RelDataType rhsType = rhs.type;

        if (lhs.typeName() == ANY || rhs.typeName() == ANY) {
            return null;
        }

        if (isNull(lhsType) && rhs.isParameter()) {
            return null;
        }
        if (lhs.isParameter() && isNull(rhsType)) {
            return null;
        }

        // Assign types to numeric literals.

        if (lhs.isNumericLiteral()) {
            lhsType = narrowestTypeFor((BigDecimal) lhs.value, rhs.isLiteral() ? null : typeName(rhsType));
        }
        if (rhs.isNumericLiteral()) {
            rhsType = narrowestTypeFor((BigDecimal) rhs.value, lhs.isLiteral() ? null : typeName(lhsType));
        }

        // Assign types to parameters.

        if (lhs.isParameter()) {
            lhsType = TYPE_FACTORY.createTypeWithNullability(rhsType, true);
        }
        if (rhs.isParameter()) {
            rhsType = TYPE_FACTORY.createTypeWithNullability(lhsType, true);
        }

        // Handle NULLs.

        if (isNull(lhsType) || isNull(rhsType)) {
            if (lhs.isLiteral() && !canRepresentLiteral(lhs, lhsType)) {
                return null;
            }
            if (rhs.isLiteral() && !canRepresentLiteral(rhs, rhsType)) {
                return null;
            }

            return new RelDataType[]{lhsType, rhsType, TYPE_FACTORY.createSqlType(NULL)};
        }

        // Infer common coerce-to type and align nullability for operands.

        RelDataType coerceTo = withHigherPrecedence(lhsType, rhsType);
        if (isNumeric(coerceTo)) {
            RelDataType[] expected = expectedNumericTypes(HazelcastTypeSystem::withHigherPrecedence, lhs, rhs, lhsType, rhsType);
            if (expected == null) {
                return null;
            }

            lhsType = expected[0];
            rhsType = expected[1];
            coerceTo = withHigherPrecedence(lhsType, rhsType);
        }

        // Validate resulting CASTs.

        if (!canCast(lhsType, coerceTo) || !canCast(rhsType, coerceTo)) {
            return null;
        }

        if (lhs.isLiteral() && !canCastLiteral(lhs, lhsType, coerceTo)) {
            return null;
        }
        if (rhs.isLiteral() && !canCastLiteral(rhs, rhsType, coerceTo)) {
            return null;
        }

        // Infer coerce-to type for operands and return type of the comparison.

        lhsType = TYPE_FACTORY.createTypeWithNullability(coerceTo, lhsType.isNullable());
        rhsType = TYPE_FACTORY.createTypeWithNullability(coerceTo, rhsType.isNullable());

        RelDataType returnType = TYPE_FACTORY.createSqlType(BOOLEAN, lhsType.isNullable() || rhsType.isNullable());
        return new RelDataType[]{lhsType, rhsType, returnType};
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        SqlTypeName typeName = types[2].getSqlTypeName();
        if (typeName == NULL) {
            return null;
        }

        Object lhs = args[0];
        if (lhs == INVALID_VALUE) {
            return INVALID_VALUE;
        }
        if (lhs == null) {
            return null;
        }

        Object rhs = args[1];
        if (rhs == INVALID_VALUE) {
            return INVALID_VALUE;
        }
        if (rhs == null) {
            return null;
        }

        int order = ((Comparable) lhs).compareTo(rhs);
        switch (mode) {
            case EQUALS:
                return order == 0;
            case NOT_EQUALS:
                return order != 0;
            case GREATER_THAN:
                return order > 0;
            case GREATER_THAN_OR_EQUAL:
                return order >= 0;
            case LESS_THAN:
                return order < 0;
            case LESS_THAN_OR_EQUAL:
                return order <= 0;
            default:
                throw new IllegalStateException("unexpected mode: " + mode);
        }
    }

}

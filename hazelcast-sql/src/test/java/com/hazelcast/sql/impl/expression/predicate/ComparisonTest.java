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

import com.hazelcast.sql.impl.expression.ExpressionTestBase;
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

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.EQUALS;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.GREATER_THAN;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.GREATER_THAN_OR_EQUAL;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.LESS_THAN;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.LESS_THAN_OR_EQUAL;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.NOT_EQUALS;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.canCast;
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
        // Infer types.

        RelDataType[] types = inferTypes(operands, false);
        if (types == null) {
            return null;
        }

        // Validate, coerce and infer return type.

        RelDataType commonType = types[types.length - 1];

        for (int i = 0; i < types.length - 1; ++i) {
            RelDataType type = types[i];

            if (typeName(type) == ANY) {
                return null;
            }

            if (!canCast(type, commonType)) {
                return null;
            }

            if (operands[i].isLiteral() && !canCastLiteral(operands[i], type, commonType)) {
                return null;
            }

            types[i] = TYPE_FACTORY.createTypeWithNullability(commonType, type.isNullable());
        }

        types[types.length - 1] = TYPE_FACTORY.createSqlType(BOOLEAN, commonType.isNullable());
        return types;
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

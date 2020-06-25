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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.CASE;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.canCast;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CaseTest extends ExpressionTestBase {

    private boolean elseExpected;

    @Test
    public void verifySimpleOneBranch() {
        elseExpected = false;
        verify(CASE, this::expectedTypes, this::expectedValues, "CASE %s WHEN TRUE THEN %s END", BOOLEAN_COLUMN, ALL);
    }

    @Test
    public void verifySimpleOneBranchElse() {
        elseExpected = true;
        verify(CASE, this::expectedTypes, this::expectedValues, "CASE %s WHEN TRUE THEN %s ELSE %s END", BOOLEAN_COLUMN, ALL,
                ALL);
    }

    @Test
    public void verifySimpleTwoBranches() {
        elseExpected = false;
        verify(CASE, this::expectedTypes, this::expectedValues, "CASE %s WHEN TRUE THEN %s WHEN FALSE THEN %s END",
                BOOLEAN_COLUMN, ALL, ALL);
    }

    @Test
    public void verifySearchedOneBranch() {
        elseExpected = false;
        verify(CASE, this::expectedTypes, this::expectedValues, "CASE WHEN %s = TRUE THEN %s END", BOOLEAN_COLUMN, ALL);
    }

    @Test
    public void verifySearchedOneBranchElse() {
        elseExpected = true;
        verify(CASE, this::expectedTypes, this::expectedValues, "CASE WHEN %s = TRUE THEN %s ELSE %s END", BOOLEAN_COLUMN, ALL,
                ALL);
    }

    @Test
    public void verifySearchedTwoBranches() {
        elseExpected = false;
        verify(CASE, this::expectedTypes, this::expectedValues, "CASE WHEN %s = TRUE THEN %s WHEN %1$s = FALSE THEN %s END",
                BOOLEAN_COLUMN, ALL, ALL);
    }

    @Override
    protected List<SqlNode> extractOperands(SqlCall call, int expectedOperandCount) {
        // apparently non-null only for aggregations which we don't support here
        assert call.operand(0) == null;

        SqlNodeList conditions = call.operand(1);
        SqlNodeList results = call.operand(2);
        assert conditions.size() == results.size();

        List<SqlNode> operands = new ArrayList<>();

        SqlCall equals = (SqlCall) conditions.get(0);
        operands.add(equals.operand(0));

        for (SqlNode result : results) {
            operands.add(result);
        }

        if (operands.size() != expectedOperandCount) {
            // add else
            assert elseExpected;
            operands.add(call.operand(3));
        }

        return operands;
    }

    private RelDataType[] expectedTypes(Operand[] operands) {
        assert operands[0].isColumn() && operands[0].typeName() == BOOLEAN;

        // Infer types.

        RelDataType[] inferredTypes = inferTypes(Arrays.copyOfRange(operands, 1, operands.length), false);
        if (inferredTypes == null) {
            return null;
        }
        RelDataType commonType = inferredTypes[inferredTypes.length - 1];

        // Validate, coerce and infer return type.

        RelDataType[] types = new RelDataType[operands.length + 1];
        types[0] = operands[0].type;
        for (int i = 0; i < inferredTypes.length - 1; ++i) {
            RelDataType type = inferredTypes[i];
            Operand operand = operands[i + 1];

            if (!canCast(type, commonType)) {
                return null;
            }

            if (operand.isLiteral()) {
                if (!canRepresentLiteral(operand, type)) {
                    return null;
                }

                if (operand.typeName() == NULL) {
                    type = TYPE_FACTORY.createTypeWithNullability(commonType, true);
                } else if (isNumeric(type)) {
                    BigDecimal numericValue = operand.numericValue();
                    //noinspection NumberEquality
                    assert numericValue != null && numericValue != INVALID_NUMERIC_VALUE;
                    if (!canRepresentLiteral(numericValue, type, commonType)) {
                        return null;
                    }
                } else if (!canRepresentLiteral(operand, commonType)) {
                    return null;
                }
            }

            types[i + 1] = type;
        }
        types[types.length - 1] = TYPE_FACTORY.createTypeWithNullability(commonType, commonType.isNullable() || !elseExpected);

        return types;
    }

    private Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        Boolean caseValue = (Boolean) args[0];

        if (TernaryLogic.isTrue(caseValue)) {
            return result(args[1], types);
        }

        if (args.length == 2) {
            assert !elseExpected;
            return null;
        }

        if (elseExpected || TernaryLogic.isFalse(caseValue)) {
            return result(args[2], types);
        }

        return null;
    }

    private static Object result(Object value, RelDataType[] types) {
        RelDataType returnType = types[types.length - 1];
        if (!isNumeric(returnType)) {
            return value;
        }

        if (value == INVALID_VALUE) {
            return INVALID_VALUE;
        }

        if (value == null) {
            return null;
        }

        switch (typeName(returnType)) {
            case TINYINT:
                return number(value).byteValue();
            case SMALLINT:
                return number(value).shortValue();
            case INTEGER:
                return number(value).intValue();
            case BIGINT:
                return number(value).longValue();
            case REAL:
                return number(value).floatValue();
            case DOUBLE:
                return number(value).doubleValue();
            case DECIMAL:
                HazelcastTestSupport.assertInstanceOf(BigDecimal.class, value);
                return value;
            default:
                throw new IllegalArgumentException("unexpected return type: " + returnType);
        }
    }

}

/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate.operators.predicate;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.param.NumericPrecedenceParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public final class HazelcastComparisonPredicateUtils {
    private HazelcastComparisonPredicateUtils() {
        // No-op
    }

    public static boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        SqlNode first = binding.operand(0);
        SqlNode second = binding.operand(1);

        HazelcastSqlValidator validator = binding.getValidator();

        RelDataType firstType = validator.deriveType(binding.getScope(), first);
        RelDataType secondType = validator.deriveType(binding.getScope(), second);

        assert firstType.getSqlTypeName() != SqlTypeName.NULL;
        assert secondType.getSqlTypeName() != SqlTypeName.NULL;

        return checkOperandTypes(binding, throwOnFailure, validator, first, firstType, second, secondType);
    }

    private static boolean checkOperandTypes(
            HazelcastCallBinding callBinding,
            boolean throwOnFailure,
            HazelcastSqlValidator validator,
            SqlNode first,
            RelDataType firstType,
            SqlNode second,
            RelDataType secondType
    ) {
        RelDataType winningType = HazelcastTypeUtils.withHigherPrecedence(firstType, secondType);

        if (winningType == firstType) {
            return checkOperandTypesWithPrecedence(
                    callBinding,
                    throwOnFailure,
                    validator,
                    first,
                    firstType,
                    second,
                    secondType,
                    1
            );
        } else {
            assert winningType == secondType;

            return checkOperandTypesWithPrecedence(
                    callBinding,
                    throwOnFailure,
                    validator,
                    second,
                    secondType,
                    first,
                    firstType,
                    0
            );
        }
    }

    private static boolean checkOperandTypesWithPrecedence(
            HazelcastCallBinding callBinding,
            boolean throwOnFailure,
            HazelcastSqlValidator validator,
            SqlNode high,
            RelDataType highType,
            SqlNode low,
            RelDataType lowType,
            int lowIndex
    ) {
        QueryDataType highHZType = HazelcastTypeUtils.toHazelcastType(highType.getSqlTypeName());
        QueryDataType lowHZType = HazelcastTypeUtils.toHazelcastType(lowType.getSqlTypeName());

        if (highHZType.getTypeFamily().isNumeric()) {
            // Set flexible parameter converter that allows TINYINT/SMALLINT/INTEGER -> BIGINT conversions
            setNumericParameterConverter(validator, high, highHZType);
            setNumericParameterConverter(validator, low, highHZType);
        }

        if (highHZType.getTypeFamily() == lowHZType.getTypeFamily()) {
            // Types are in the same family, do nothing.
            return true;
        }

        boolean valid = bothOperandsAreNumeric(highHZType, lowHZType)
                || bothOperandsAreTemporalAndLowOperandCanBeConvertedToHighOperand(highHZType, lowHZType)
                || highOperandIsTemporalAndLowOperandIsLiteralOfVarcharType(highHZType, lowHZType, low);

        if (!valid) {
            // Types cannot be converted to each other, throw.
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            } else {
                return false;
            }
        }

        // Types are in the same group, cast lower to higher.
        RelDataType newLowType = validator.getTypeFactory().createTypeWithNullability(highType, lowType.isNullable());

        validator.getTypeCoercion().coerceOperandType(callBinding.getScope(), callBinding.getCall(), lowIndex, newLowType);

        return true;
    }

    private static boolean bothOperandsAreNumeric(QueryDataType highHZType, QueryDataType lowHZType) {
        return (highHZType.getTypeFamily().isNumeric() && lowHZType.getTypeFamily().isNumeric());
    }

    private static boolean bothOperandsAreTemporalAndLowOperandCanBeConvertedToHighOperand(QueryDataType highHZType,
                                                                                           QueryDataType lowHZType) {
        return highHZType.getTypeFamily().isTemporal()
                && lowHZType.getTypeFamily().isTemporal()
                && lowHZType.getConverter().canConvertTo(highHZType.getTypeFamily());
    }

    private static boolean highOperandIsTemporalAndLowOperandIsLiteralOfVarcharType(QueryDataType highHZType,
                                                                                    QueryDataType lowHZType, SqlNode low) {
        return highHZType.getTypeFamily().isTemporal()
                && lowHZType.getTypeFamily() == QueryDataTypeFamily.VARCHAR
                && low instanceof SqlLiteral;
    }

    private static void setNumericParameterConverter(HazelcastSqlValidator validator, SqlNode node, QueryDataType type) {
        if (node.getKind() == SqlKind.DYNAMIC_PARAM) {
            SqlDynamicParam node0 = (SqlDynamicParam) node;

            ParameterConverter converter = new NumericPrecedenceParameterConverter(
                    node0.getIndex(),
                    node.getParserPosition(),
                    type
            );

            validator.setParameterConverter(node0.getIndex(), converter);
        }
    }
}

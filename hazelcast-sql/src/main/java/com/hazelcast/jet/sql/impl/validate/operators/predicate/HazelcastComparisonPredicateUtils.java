/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.validate.operators.predicate;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.jet.sql.impl.validate.param.NumericPrecedenceParameterConverter;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

public final class HazelcastComparisonPredicateUtils {
    private HazelcastComparisonPredicateUtils() {
    }

    public static boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        SqlNode first = binding.operand(0);
        SqlNode second = binding.operand(1);

        HazelcastSqlValidator validator = binding.getValidator();

        RelDataType firstType = validator.deriveType(binding.getScope(), first);
        RelDataType secondType = validator.deriveType(binding.getScope(), second);

        assert !firstType.equals(validator.getUnknownType());
        assert !secondType.equals(validator.getUnknownType());

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
        QueryDataType highHZType = HazelcastTypeUtils.toHazelcastType(highType);
        QueryDataType lowHZType = HazelcastTypeUtils.toHazelcastType(lowType);

        if (highHZType.getTypeFamily().isNumeric()) {
            // Set flexible parameter converter that allows TINYINT/SMALLINT/INTEGER -> BIGINT conversions
            setNumericParameterConverter(validator, high, highHZType);
            setNumericParameterConverter(validator, low, highHZType);
        }

        boolean valid = validator
                .getTypeCoercion()
                .rowTypeElementCoercion(
                        callBinding.getScope(),
                        low,
                        highType,
                        lowOperandNode -> callBinding.getCall().setOperand(lowIndex, lowOperandNode));

        if (valid && highHZType == QueryDataType.OBJECT && lowHZType != QueryDataType.OBJECT) {
            valid = false;
        }

        // Types cannot be converted to each other.
        if (!valid && throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        }

        return valid;
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

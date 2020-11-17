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

package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.literal.HazelcastSqlLiteral;
import com.hazelcast.sql.impl.calcite.literal.HazelcastSqlLiteralFunction;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.param.NumericPrecedenceParameterConverter;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public final class DoubleOperandChecker implements OperandChecker {

    public static final DoubleOperandChecker INSTANCE = new DoubleOperandChecker();

    private DoubleOperandChecker() {
        // No-op
    }

    @Override
    public boolean check(SqlCallBindingOverride callBinding, boolean throwOnFailure, int i) {
        HazelcastSqlValidator validator = callBinding.getValidator();

        SqlNode operand = callBinding.operand(i);

        if (operand.getKind() == SqlKind.DYNAMIC_PARAM) {
            return checkParameter(validator, (SqlDynamicParam) operand);
        } else {
            RelDataType operandType = validator.deriveType(callBinding.getScope(), operand);

            if (operand instanceof SqlBasicCall) {
                SqlBasicCall operandCall = (SqlBasicCall) operand;

                if (operandCall.getOperator() == HazelcastSqlLiteralFunction.INSTANCE) {
                    return checkLiteral(validator, callBinding, throwOnFailure, operandCall, operandType, operandCall.operand(0));
                }
            }

            if (operandType.getSqlTypeName() == SqlTypeName.DOUBLE) {
                return true;
            } else {
                if (SqlToQueryType.map(operandType.getSqlTypeName()).getTypeFamily().isNumeric()) {
                    RelDataType doubleType = SqlNodeUtil.createType(
                        validator.getTypeFactory(),
                        SqlTypeName.DOUBLE,
                        operandType.isNullable()
                    );

                    validator.getTypeCoercion().coerceOperandType(callBinding.getScope(), callBinding.getCall(), 0, doubleType);
                    validator.setKnownAndValidatedNodeType(operand, doubleType);

                    return true;
                }

                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }
        }
    }

    private boolean checkLiteral(
        HazelcastSqlValidator validator,
        SqlCallBindingOverride callBinding,
        boolean throwOnFailure,
        SqlNode operand,
        RelDataType operandType,
        HazelcastSqlLiteral literal
    ) {
        if (literal.getTypeName() == SqlTypeName.DOUBLE) {
            return true;
        }

        if (literal.getTypeName() == SqlTypeName.NULL) {
            RelDataType booleanNullableType = SqlNodeUtil.createType(validator.getTypeFactory(), SqlTypeName.BOOLEAN, true);
            validator.setKnownAndValidatedNodeType(operand, booleanNullableType);

            return true;
        }

        if (SqlToQueryType.map(operandType.getSqlTypeName()).getTypeFamily().isNumeric()) {
            RelDataType doubleType = SqlNodeUtil.createType(
                validator.getTypeFactory(),
                SqlTypeName.DOUBLE,
                operandType.isNullable()
            );

            validator.getTypeCoercion().coerceOperandType(callBinding.getScope(), callBinding.getCall(), 0, doubleType);
            validator.setKnownAndValidatedNodeType(operand, doubleType);

            return true;
        }

        // Cannot convert the literal to numeric, validation fails
        if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        } else {
            return false;
        }
    }

    private boolean checkParameter(HazelcastSqlValidator validator, SqlDynamicParam operand) {
        // Set parameter type.
        RelDataType type = SqlNodeUtil.createType(validator.getTypeFactory(), SqlTypeName.BOOLEAN, true);
        validator.setKnownAndValidatedNodeType(operand, type);

        // Set parameter converter.
        ParameterConverter converter = new NumericPrecedenceParameterConverter(
            operand.getIndex(),
            operand.getParserPosition(),
            QueryDataType.DOUBLE
        );

        validator.setParameterConverter(operand.getIndex(), converter);

        return true;
    }
}

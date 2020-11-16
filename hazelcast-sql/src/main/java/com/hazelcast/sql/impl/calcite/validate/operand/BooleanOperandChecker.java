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

import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.param.StrictParameterConverter;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public final class BooleanOperandChecker implements OperandChecker {

    public static final BooleanOperandChecker INSTANCE = new BooleanOperandChecker();

    private BooleanOperandChecker() {
        // No-op
    }

    @Override
    public boolean check(SqlCallBindingOverride callBinding, boolean throwOnFailure, int i) {
        HazelcastSqlValidator validator = callBinding.getValidator();

        SqlNode operand = callBinding.operand(i);

        if (operand.getKind() == SqlKind.LITERAL) {
            return checkLiteral(validator, callBinding, throwOnFailure, i, (SqlLiteral) operand);
        } else if (operand.getKind() == SqlKind.DYNAMIC_PARAM) {
            return checkParameter(validator, (SqlDynamicParam) operand);
        } else {
            RelDataType operandType = validator.deriveType(callBinding.getScope(), operand);

            if (operandType.getSqlTypeName() == SqlTypeName.BOOLEAN) {
                return true;
            } else {
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
        int operandIndex,
        SqlLiteral operand
    ) {
        BiTuple<Boolean, Boolean> value = SqlNodeUtil.booleanValue(operand);

        if (value.element1()) {
            // We were able to cast the literal to BOOLEAN value. Validation will definitely succeed
            if (operand.getTypeName() != SqlTypeName.BOOLEAN) {
                // If the original operand type is not BOOLEAN, then mark it as BOOLEAN forcefully
                // For example, this is needed for ['true'] literal that is originally handled as VARCHAR
                boolean nullable = value.element2() == null;

                operand = nullable ? SqlLiteral.createUnknown(operand.getParserPosition())
                    : SqlLiteral.createBoolean(value.element2(), operand.getParserPosition());

                RelDataType booleanType = SqlNodeUtil.createType(
                    validator.getTypeFactory(),
                    SqlTypeName.BOOLEAN,
                    nullable
                );

                callBinding.getCall().setOperand(operandIndex, operand);

                validator.setKnownAndValidatedNodeType(operand, booleanType);
            }

            return true;
        } else {
            // Cannot convert the literal to BOOLEAN, validation fails
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            } else {
                return false;
            }
        }
    }

    private boolean checkParameter(HazelcastSqlValidator validator, SqlDynamicParam operand) {
        // Set parameter type.
        RelDataType type = SqlNodeUtil.createType(validator.getTypeFactory(), SqlTypeName.BOOLEAN, true);
        validator.setKnownAndValidatedNodeType(operand, type);

        // Set parameter converter.
        ParameterConverter converter = new StrictParameterConverter(
            operand.getIndex(),
            operand.getParserPosition(),
            QueryDataType.BOOLEAN
        );

        validator.setParameterConverter(operand.getIndex(), converter);

        return true;
    }
}

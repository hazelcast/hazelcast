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

package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public abstract class AbstractOperandChecker implements OperandChecker {
    protected AbstractOperandChecker() {
        // No-op
    }

    @Override
    public boolean check(HazelcastCallBinding callBinding, boolean throwOnFailure, int operandIndex) {
        HazelcastSqlValidator validator = callBinding.getValidator();

        SqlNode operand = callBinding.getCall().operand(operandIndex);

        if (operand.getKind() == SqlKind.DYNAMIC_PARAM) {
            validateDynamicParam((SqlDynamicParam) operand, validator);
            return true;
        }
        if (operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
            SqlNode value = ((SqlCall) operand).operand(0);
            if (value.getKind() == SqlKind.DYNAMIC_PARAM) {
                validateDynamicParam((SqlDynamicParam) value, validator);
                return true;
            }
        }

        RelDataType operandType = validator.deriveType(callBinding.getScope(), operand);

        assert operandType.getSqlTypeName() != SqlTypeName.NULL : "Operand type is not resolved";

        // Handle type match
        if (matchesTargetType(operandType)) {
            return true;
        }

        // Handle coercion if possible
        if (coerce(validator, callBinding, operand, operandType, operandIndex)) {
            return true;
        }

        // Failed
        if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        } else {
            return false;
        }
    }

    private void validateDynamicParam(SqlDynamicParam operand, HazelcastSqlValidator validator) {
        // Set parameter type
        RelDataType type = getTargetType(validator.getTypeFactory(), true);
        validator.setValidatedNodeType(operand, type);

        // Set parameter converter
        ParameterConverter converter = parameterConverter(operand);
        validator.setParameterConverter(operand.getIndex(), converter);
    }

    protected abstract RelDataType getTargetType(RelDataTypeFactory factory, boolean nullable);

    protected abstract boolean matchesTargetType(RelDataType operandType);

    protected abstract ParameterConverter parameterConverter(SqlDynamicParam operand);

    protected abstract boolean coerce(
            HazelcastSqlValidator validator,
            HazelcastCallBinding callBinding,
            SqlNode operand,
            RelDataType operandType,
            int operandIndex
    );
}

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

package com.hazelcast.jet.sql.impl.validate.operand;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.ParameterConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isNullOrUnknown;

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

        assert !isNullOrUnknown(operandType.getSqlTypeName()) : "Operand type is not resolved";

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

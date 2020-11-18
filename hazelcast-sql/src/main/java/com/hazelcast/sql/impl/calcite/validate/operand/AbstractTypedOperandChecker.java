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
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public abstract class AbstractTypedOperandChecker implements OperandChecker {

    protected final SqlTypeName typeName;

    public AbstractTypedOperandChecker(SqlTypeName typeName) {
        this.typeName = typeName;
    }

    @Override
    public boolean check(SqlCallBindingOverride callBinding, boolean throwOnFailure, int operandIndex) {
        HazelcastSqlValidator validator = callBinding.getValidator();

        SqlNode operand = callBinding.operand(operandIndex);

        // Handle parameter
        if (operand.getKind() == SqlKind.DYNAMIC_PARAM) {
            return checkParameter(validator, (SqlDynamicParam) operand);
        }

        // Handle direct type match
        RelDataType operandType = validator.deriveType(callBinding.getScope(), operand);

        if (operandType.getSqlTypeName() == typeName) {
            return true;
        }

        // Handle literal
        RelDataType literalType = SqlNodeUtil.literalType(operand, (HazelcastTypeFactory) validator.getTypeFactory());

        if (literalType != null) {
            return checkLiteral(validator, callBinding, throwOnFailure, operand, operandType, operandIndex);
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

    private boolean checkLiteral(
        HazelcastSqlValidator validator,
        SqlCallBindingOverride callBinding,
        boolean throwOnFailure,
        SqlNode operand,
        RelDataType operandType,
        int operandIndex
    ) {
        // Handle NULL literal
        if (operandType.getSqlTypeName() == SqlTypeName.NULL) {
            validator.setKnownAndValidatedNodeType(
                operand,
                SqlNodeUtil.createType(validator.getTypeFactory(), typeName, true)
            );

            return true;
        }

        // Coerce if possible
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

    private boolean checkParameter(HazelcastSqlValidator validator, SqlDynamicParam operand) {
        // Set parameter type
        RelDataType type = SqlNodeUtil.createType(validator.getTypeFactory(), typeName, true);
        validator.setKnownAndValidatedNodeType(operand, type);

        // Set parameter converter
        ParameterConverter converter = parameterConverter(operand);
        validator.setParameterConverter(operand.getIndex(), converter);

        return true;
    }

    protected abstract ParameterConverter parameterConverter(SqlDynamicParam operand);

    protected boolean coerce(
        HazelcastSqlValidator validator,
        SqlCallBindingOverride callBinding,
        SqlNode operand,
        RelDataType operandType,
        int operandIndex
    ) {
        return false;
    }
}

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

package com.hazelcast.sql.impl.calcite.validate;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.Collection;
import java.util.StringJoiner;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastResources.RESOURCES;

/**
 * A custom implementation of {@link SqlCallBinding} that produces a custom error message for signature validation
 * errors.
 */
public class HazelcastCallBinding extends SqlCallBinding {
    public HazelcastCallBinding(SqlCallBinding binding) {
        super(binding.getValidator(), binding.getScope(), binding.getCall());
    }

    @Override
    public HazelcastSqlValidator getValidator() {
        return (HazelcastSqlValidator) super.getValidator();
    }

    @Override
    public CalciteException newValidationSignatureError() {
        SqlOperator operator = getOperator();
        SqlValidator validator = getValidator();
        SqlCall call = getCall();

        String operandTypes = getOperandTypes(validator, call, getScope());

        Resources.ExInst<SqlValidatorException> error;

        String operatorName = '\'' + operator.getName() + '\'';

        switch (operator.getSyntax()) {
            case FUNCTION:
            case FUNCTION_STAR:
            case FUNCTION_ID:
                error = RESOURCES.invalidFunctionOperands(operatorName, operandTypes);

                break;

            default:
                error = RESOURCES.invalidOperatorOperands(operatorName, operandTypes);
        }

        return validator.newValidationError(call, error);
    }

    private static String getOperandTypes(
            SqlValidator validator,
            SqlCall call,
            SqlValidatorScope scope
    ) {
        StringJoiner res = new StringJoiner(", ", "[", "]");

        for (SqlNode operand : getOperands(call)) {
            RelDataType calciteType = validator.deriveType(scope, operand);

            String typeName;

            if (calciteType.getSqlTypeName() == SqlTypeName.NULL) {
                typeName = validator.getUnknownType().toString();
            } else {
                QueryDataType hazelcastType = HazelcastTypeUtils.toHazelcastType(calciteType.getSqlTypeName());
                if (hazelcastType.getTypeFamily().getPublicType() != null) {
                    typeName = hazelcastType.getTypeFamily().getPublicType().name();
                } else {
                    typeName = calciteType.getSqlTypeName().toString();
                }
            }

            res.add(typeName);
        }

        return res.toString();
    }

    private static Collection<SqlNode> getOperands(SqlCall call) {
        SqlOperator operator = call.getOperator();

        if (operator instanceof HazelcastCallBindingSignatureErrorAware) {
            return ((HazelcastCallBindingSignatureErrorAware) operator).getOperandsForSignatureError(call);
        } else {
            return call.getOperandList();
        }
    }
}

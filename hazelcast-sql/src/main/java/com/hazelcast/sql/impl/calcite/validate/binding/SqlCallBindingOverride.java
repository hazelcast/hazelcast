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

package com.hazelcast.sql.impl.calcite.validate.binding;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastResources.RESOURCES;

/**
 * An overridden implementation of {@link SqlCallBinding} that produces a custom error message for signature validation
 * errors.
 * <p>
 * Operators must either use {@link SqlCallBindingOverrideOperandChecker} that replaces the original binding with
 * {@code SqlCallBindingOverride}, or define the {@link SqlCallBindingManualOverride} marker interface and replace
 * the original binding manually in the {@link SqlOperator#checkOperandTypes(SqlCallBinding, boolean)}.
 */
public class SqlCallBindingOverride extends SqlCallBinding {
    public SqlCallBindingOverride(SqlCallBinding binding) {
        super(binding.getValidator(), binding.getScope(), binding.getCall());
    }

    @Override
    public CalciteException newValidationSignatureError() {
        SqlOperator operator = getOperator();
        SqlValidator validator = getValidator();
        SqlCall call = getCall();

        String signature = getCallSignature(operator, validator, call, getScope());

        Resources.ExInst<SqlValidatorException> error;

        switch (operator.getSyntax()) {
            case FUNCTION:
            case FUNCTION_STAR:
            case FUNCTION_ID:
                error = RESOURCES.canNotApplyOperandsToFunction(signature);

                break;

            default:
                error = RESOURCES.canNotApplyOperandsToOperator(signature);
        }

        return validator.newValidationError(call, error);
    }

    private static String getCallSignature(
        SqlOperator operator,
        SqlValidator validator,
        SqlCall call,
        SqlValidatorScope scope
    ) {
        List<String> operandTypes = new ArrayList<>();

        for (SqlNode operand : call.getOperandList()) {
            RelDataType operandType = validator.deriveType(scope, operand);

            assert operandType != null;

            operandTypes.add(operandType.toString());
        }

        return SqlUtil.getOperatorSignature(operator, operandTypes);
    }
}

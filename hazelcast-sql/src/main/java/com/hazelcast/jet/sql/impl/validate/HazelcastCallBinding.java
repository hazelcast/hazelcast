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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
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

import static com.hazelcast.jet.sql.impl.validate.HazelcastResources.RESOURCES;

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

    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    private static String getOperandTypes(
            SqlValidator validator,
            SqlCall call,
            SqlValidatorScope scope
    ) {
        StringJoiner res = new StringJoiner(", ", "[", "]");

        for (SqlNode operand : getOperands(call)) {
            RelDataType calciteType = validator.deriveType(scope, operand);

            String typeName;

            SqlTypeName sqlTypeName = calciteType.getSqlTypeName();
            if (sqlTypeName == SqlTypeName.NULL
                    || sqlTypeName == SqlTypeName.UNKNOWN
                    || sqlTypeName == SqlTypeName.ROW
                    || sqlTypeName == SqlTypeName.COLUMN_LIST
                    || sqlTypeName == SqlTypeName.SYMBOL
                    || sqlTypeName == SqlTypeName.CURSOR
            ) {
                typeName = sqlTypeName.toString();
            } else {
                QueryDataType hazelcastType = HazelcastTypeUtils.toHazelcastType(calciteType);
                if (hazelcastType.getTypeFamily().getPublicType() != null) {
                    typeName = hazelcastType.getTypeFamily().getPublicType().toString();
                } else {
                    typeName = sqlTypeName.toString();
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

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
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.param.NumericPrecedenceParameterConverter;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public final class NumericOperandChecker extends AbstractTypedOperandChecker {

    public static final NumericOperandChecker TINYINT = new NumericOperandChecker(SqlTypeName.TINYINT);
    public static final NumericOperandChecker SMALLINT = new NumericOperandChecker(SqlTypeName.SMALLINT);
    public static final NumericOperandChecker INTEGER = new NumericOperandChecker(SqlTypeName.INTEGER);
    public static final NumericOperandChecker BIGINT = new NumericOperandChecker(SqlTypeName.BIGINT);
    public static final NumericOperandChecker DECIMAL = new NumericOperandChecker(SqlTypeName.DECIMAL);
    public static final NumericOperandChecker REAL = new NumericOperandChecker(SqlTypeName.REAL);
    public static final NumericOperandChecker DOUBLE = new NumericOperandChecker(SqlTypeName.DOUBLE);

    private final QueryDataType type;

    private NumericOperandChecker(SqlTypeName typeName) {
        super(typeName);

         type = SqlToQueryType.map(typeName);

         assert type.getTypeFamily().isNumeric();
    }

    @Override
    protected ParameterConverter parameterConverter(SqlDynamicParam operand) {
        return new NumericPrecedenceParameterConverter(
            operand.getIndex(),
            operand.getParserPosition(),
            type
        );
    }

    @Override
    protected boolean coerce(
        HazelcastSqlValidator validator,
        SqlCallBindingOverride callBinding,
        SqlNode operand,
        RelDataType operandType,
        int operandIndex
    ) {
        QueryDataType operandType0 = SqlToQueryType.map(operandType.getSqlTypeName());

        if (!operandType0.getTypeFamily().isNumeric()) {
            // We allow coercion only within numeric types.
            return false;
        }

        if (type.getTypeFamily().getPrecedence() < operandType0.getTypeFamily().getPrecedence()) {
            // Cannot convert type with higher precedence to lower precedence (e.g. DOUBLE to INTEGER)
            return false;
        }

        // Otherwise we are good to go. Construct the new type of the operand.
        RelDataType newOperandType = SqlNodeUtil.createType(validator.getTypeFactory(), typeName, operandType.isNullable());

        // Perform coercion
        // TODO: Can it fail? Should we check for boolean result?
        validator.getTypeCoercion().coerceOperandType(
            callBinding.getScope(),
            callBinding.getCall(),
            operandIndex,
            newOperandType
        );

        // Let validator know about the type change.
        validator.setKnownAndValidatedNodeType(operand, newOperandType);

        return true;
    }

    public static boolean checkNumeric(SqlCallBindingOverride binding, boolean throwOnFailure, int index) {
        // Resolve a numeric checker for the operand
        SqlNode operand = binding.operand(index);

        RelDataType operandType = binding.getValidator().deriveType(binding.getScope(), operand);

        NumericOperandChecker checker = checkerForTypeName(operandType.getSqlTypeName());

        if (checker != null) {
            // Numeric checker is found, invoke
            return checker.check(binding, throwOnFailure, index);
        } else {
            // Not a numeric type, fail.
            if (throwOnFailure) {
                throw binding.newValidationSignatureError();
            } else {
                return false;
            }
        }
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static NumericOperandChecker checkerForTypeName(SqlTypeName typeName) {
        switch (typeName) {
            case NULL:
                // Upcast unknown operand (parameter, NULL literal) to BIGINT
                return BIGINT;

            case TINYINT:
                return TINYINT;

            case SMALLINT:
                return SMALLINT;

            case INTEGER:
                return INTEGER;

            case BIGINT:
                return BIGINT;

            case DECIMAL:
                return DECIMAL;

            case REAL:
            case FLOAT:
                return REAL;

            case DOUBLE:
                return DOUBLE;

            default:
                return null;
        }
    }
}

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

public final class NumericTypedOperandChecker extends AbstractTypedOperandChecker {

    public static final NumericTypedOperandChecker TINYINT = new NumericTypedOperandChecker(SqlTypeName.TINYINT);
    public static final NumericTypedOperandChecker SMALLINT = new NumericTypedOperandChecker(SqlTypeName.SMALLINT);
    public static final NumericTypedOperandChecker INTEGER = new NumericTypedOperandChecker(SqlTypeName.INTEGER);
    public static final NumericTypedOperandChecker BIGINT = new NumericTypedOperandChecker(SqlTypeName.BIGINT);
    public static final NumericTypedOperandChecker DECIMAL = new NumericTypedOperandChecker(SqlTypeName.DECIMAL);
    public static final NumericTypedOperandChecker REAL = new NumericTypedOperandChecker(SqlTypeName.REAL);
    public static final NumericTypedOperandChecker DOUBLE = new NumericTypedOperandChecker(SqlTypeName.DOUBLE);

    private final QueryDataType type;

    private NumericTypedOperandChecker(SqlTypeName typeName) {
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
}

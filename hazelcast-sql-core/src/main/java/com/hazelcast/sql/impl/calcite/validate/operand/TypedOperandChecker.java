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
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.param.NumericPrecedenceParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.param.StrictParameterConverter;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public final class TypedOperandChecker extends AbstractOperandChecker {

    public static final TypedOperandChecker BOOLEAN = new TypedOperandChecker(SqlTypeName.BOOLEAN);
    public static final TypedOperandChecker VARCHAR = new TypedOperandChecker(SqlTypeName.VARCHAR);
    public static final TypedOperandChecker TINYINT = new TypedOperandChecker(SqlTypeName.TINYINT);
    public static final TypedOperandChecker SMALLINT = new TypedOperandChecker(SqlTypeName.SMALLINT);
    public static final TypedOperandChecker INTEGER = new TypedOperandChecker(SqlTypeName.INTEGER);
    public static final TypedOperandChecker BIGINT = new TypedOperandChecker(SqlTypeName.BIGINT);
    public static final TypedOperandChecker DECIMAL = new TypedOperandChecker(SqlTypeName.DECIMAL);
    public static final TypedOperandChecker REAL = new TypedOperandChecker(SqlTypeName.REAL);
    public static final TypedOperandChecker DOUBLE = new TypedOperandChecker(SqlTypeName.DOUBLE);

    private final SqlTypeName typeName;
    private final RelDataType type;

    private TypedOperandChecker(SqlTypeName typeName) {
        this.typeName = typeName;

        type = null;
    }

    private TypedOperandChecker(RelDataType type) {
        typeName = type.getSqlTypeName();

        this.type = type;
    }

    public static TypedOperandChecker forType(RelDataType type) {
        return new TypedOperandChecker(type);
    }

    @Override
    protected boolean matchesTargetType(RelDataType operandType) {
        if (type != null) {
            return type.equals(operandType);
        } else {
            return operandType.getSqlTypeName() == typeName;
        }
    }

    @Override
    protected RelDataType getTargetType(RelDataTypeFactory factory, boolean nullable) {
        if (type != null) {
            return factory.createTypeWithNullability(type, nullable);
        } else {
            return HazelcastTypeUtils.createType(factory, typeName, nullable);
        }
    }

    @Override
    protected boolean coerce(
        HazelcastSqlValidator validator,
        HazelcastCallBinding callBinding,
        SqlNode operand,
        RelDataType operandType,
        int operandIndex
    ) {
        QueryDataType targetType0 = getTargetHazelcastType();
        QueryDataType operandType0 = HazelcastTypeUtils.toHazelcastType(operandType.getSqlTypeName());

        if (!isNumeric() || !operandType0.getTypeFamily().isNumeric()) {
            // Do not coerce non-numeric types
            return false;
        }

        if (targetType0.getTypeFamily().getPrecedence() < operandType0.getTypeFamily().getPrecedence()) {
            // Cannot convert type with higher precedence to lower precedence (e.g. DOUBLE to INTEGER)
            return false;
        }

        // Otherwise we are good to go. Construct the new type of the operand.
        RelDataType newOperandType = getTargetType(validator.getTypeFactory(), operandType.isNullable());

        // Perform coercion
        validator.getTypeCoercion().coerceOperandType(
            callBinding.getScope(),
            callBinding.getCall(),
            operandIndex,
            newOperandType
        );

        return true;
    }

    @Override
    protected ParameterConverter parameterConverter(SqlDynamicParam operand) {
        QueryDataType hazelcastType = getTargetHazelcastType();

        if (isNumeric()) {
            return new NumericPrecedenceParameterConverter(
                operand.getIndex(),
                operand.getParserPosition(),
                hazelcastType
            );
        } else {
            return new StrictParameterConverter(
                operand.getIndex(),
                operand.getParserPosition(),
                hazelcastType
            );
        }
    }

    private QueryDataType getTargetHazelcastType() {
        return HazelcastTypeUtils.toHazelcastType(typeName);
    }

    public boolean isNumeric() {
        return getTargetHazelcastType().getTypeFamily().isNumeric();
    }
}

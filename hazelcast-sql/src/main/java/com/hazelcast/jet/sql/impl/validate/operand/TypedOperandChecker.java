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

import com.hazelcast.jet.sql.impl.validate.types.HazelcastJsonType;
import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.jet.sql.impl.validate.param.AbstractParameterConverter;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isNumericType;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isTemporalType;

public class TypedOperandChecker extends AbstractOperandChecker {

    public static final TypedOperandChecker BOOLEAN = new TypedOperandChecker(SqlTypeName.BOOLEAN);
    public static final TypedOperandChecker VARCHAR = new TypedOperandChecker(SqlTypeName.VARCHAR);
    public static final TypedOperandChecker TINYINT = new TypedOperandChecker(SqlTypeName.TINYINT);
    public static final TypedOperandChecker SMALLINT = new TypedOperandChecker(SqlTypeName.SMALLINT);
    public static final TypedOperandChecker INTEGER = new TypedOperandChecker(SqlTypeName.INTEGER);
    public static final TypedOperandChecker BIGINT = new TypedOperandChecker(SqlTypeName.BIGINT);
    public static final TypedOperandChecker DECIMAL = new TypedOperandChecker(SqlTypeName.DECIMAL);
    public static final TypedOperandChecker REAL = new TypedOperandChecker(SqlTypeName.REAL);
    public static final TypedOperandChecker DOUBLE = new TypedOperandChecker(SqlTypeName.DOUBLE);
    public static final TypedOperandChecker TIMESTAMP_WITH_TIME_ZONE =
            new TypedOperandChecker(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    public static final TypedOperandChecker MAP = new TypedOperandChecker(SqlTypeName.MAP);
    public static final TypedOperandChecker COLUMN_LIST = new TypedOperandChecker(SqlTypeName.COLUMN_LIST);
    public static final TypedOperandChecker ROW = new TypedOperandChecker(SqlTypeName.ROW);
    public static final TypedOperandChecker SYMBOL = new TypedOperandChecker(SqlTypeName.SYMBOL);
    public static final TypedOperandChecker JSON = new TypedOperandChecker(HazelcastJsonType.TYPE);

    protected final SqlTypeName targetTypeName;
    protected final RelDataType type;

    protected TypedOperandChecker(SqlTypeName targetTypeName) {
        this.targetTypeName = targetTypeName;

        type = null;
    }

    protected TypedOperandChecker(RelDataType type) {
        targetTypeName = type.getSqlTypeName();

        this.type = type;
    }

    public static TypedOperandChecker forType(RelDataType type) {
        return new TypedOperandChecker(type);
    }

    @Override
    protected boolean matchesTargetType(RelDataType operandType) {
        if (type != null && type.getSqlTypeName().equals(SqlTypeName.OTHER)) {
            return type.getFamily().equals(operandType.getFamily());
        } else {
            return operandType.getSqlTypeName() == targetTypeName;
        }
    }

    @Override
    protected RelDataType getTargetType(RelDataTypeFactory factory, boolean nullable) {
        if (type != null) {
            return factory.createTypeWithNullability(type, nullable);
        } else {
            return HazelcastTypeUtils.createType(factory, targetTypeName, nullable);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    protected boolean coerce(
            HazelcastSqlValidator validator,
            HazelcastCallBinding callBinding,
            SqlNode operand,
            RelDataType operandType,
            int operandIndex
    ) {
        if (targetTypeName == SqlTypeName.ROW || targetTypeName == SqlTypeName.COLUMN_LIST) {
            return false;
        }

        QueryDataType targetType0 = getTargetHazelcastType();
        QueryDataType operandType0 = HazelcastTypeUtils.toHazelcastType(operandType);

        boolean canCoerce = isTemporalType(operandType) && isTemporalType(targetTypeName)
                || isNumericType(operandType) && isNumericType(targetTypeName)
                // we can assign VARCHAR to JSON fields
                || operandType.getSqlTypeName() == SqlTypeName.VARCHAR
                && type != null && type.getFamily() == HazelcastJsonType.FAMILY;

        if (!canCoerce) {
            return false;
        }

        if (targetType0.getTypeFamily().getPrecedence() < operandType0.getTypeFamily().getPrecedence()) {
            // Cannot convert type with higher precedence to lower precedence (e.g. DOUBLE to INTEGER)
            return false;
        }

        // Otherwise, we are good to go. Construct the new type of the operand.
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
        QueryDataType targetHazelcastType = getTargetHazelcastType();
        return AbstractParameterConverter.from(targetHazelcastType,  operand.getIndex(), operand.getParserPosition());
    }

    public boolean isNumeric() {
        return getTargetHazelcastType().getTypeFamily().isNumeric();
    }

    private QueryDataType getTargetHazelcastType() {
        return type != null
                ? HazelcastTypeUtils.toHazelcastType(type)
                : HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(targetTypeName);
    }
}

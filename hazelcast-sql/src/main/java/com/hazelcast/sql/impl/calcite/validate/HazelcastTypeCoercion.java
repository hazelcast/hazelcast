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

package com.hazelcast.sql.impl.calcite.validate;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerTypeNameSpec;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil.isLiteralRemoveMe;
import static com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil.isParameter;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isChar;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isTemporal;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.typeName;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;

/**
 * Provides custom coercion strategies supporting {@link HazelcastIntegerType}
 * and assigning more precise types comparing to the standard Calcite coercion.
 */
public final class HazelcastTypeCoercion extends TypeCoercionImpl {

    private static final HazelcastTypeFactory TYPE_FACTORY = HazelcastTypeFactory.INSTANCE;

    public HazelcastTypeCoercion(HazelcastSqlValidator validator) {
        super(TYPE_FACTORY, validator);
    }

    @Override
    public boolean binaryArithmeticCoercion(SqlCallBinding binding) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public boolean binaryComparisonCoercion(SqlCallBinding binding) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public boolean rowTypeCoercion(SqlValidatorScope scope, SqlNode query, int columnIndex, RelDataType targetType) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public boolean caseWhenCoercion(SqlCallBinding callBinding) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public boolean inOperationCoercion(SqlCallBinding binding) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public boolean builtinFunctionCoercion(
        SqlCallBinding binding,
        List<RelDataType> operandTypes,
        List<SqlTypeFamily> expectedFamilies
    ) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public boolean userDefinedFunctionCoercion(SqlValidatorScope scope, SqlCall call, SqlFunction function) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public boolean querySourceCoercion(
        SqlValidatorScope scope,
        RelDataType sourceRowType,
        RelDataType targetRowType,
        SqlNode query
    ) {
        throw new UnsupportedOperationException("Should not be called");
    }

    // TODO: Review
    @Override
    public RelDataType implicitCast(RelDataType in, SqlTypeFamily expected) {
        // enables implicit conversion from CHAR to BOOLEAN
        if (CHAR_TYPES.contains(typeName(in)) && expected == SqlTypeFamily.BOOLEAN) {
            return TYPE_FACTORY.createSqlType(BOOLEAN, in.isNullable());
        }

        return super.implicitCast(in, expected);
    }

    @Override
    public boolean coerceOperandType(SqlValidatorScope scope, SqlCall call, int index, RelDataType targetType) {
        SqlNode operand = call.getOperandList().get(index);

        // just update the inferred type if casting is not needed
        if (!needToCast(scope, operand, targetType)) {
            updateInferredType(operand, targetType);

            return false;
        }

        SqlDataTypeSpec targetTypeSpec;

        if (targetType instanceof HazelcastIntegerType) {
            HazelcastIntegerTypeNameSpec targetTypeNameSpec = new HazelcastIntegerTypeNameSpec((HazelcastIntegerType) targetType);

            targetTypeSpec = new SqlDataTypeSpec(
                targetTypeNameSpec,
                SqlParserPos.ZERO
            );
        } else {
            targetTypeSpec = SqlTypeUtil.convertTypeToSpec(targetType);
        }

        SqlNode cast = HazelcastSqlOperatorTable.CAST.createCall(SqlParserPos.ZERO, operand, targetTypeSpec);

        call.setOperand(index, cast);

        validator.deriveType(scope, cast);

        return true;
    }

    // TODO: Review (move to separate method?)
    @SuppressWarnings("checkstyle:NPathComplexity")
    @Override
    protected boolean needToCast(SqlValidatorScope scope, SqlNode node, RelDataType to) {
        RelDataType from = validator.deriveType(scope, node);

        if (typeName(from) == typeName(to)) {
            // already of the same type
            return false;
        }

        if (typeName(from) == NULL || SqlUtil.isNullLiteral(node, false)) {
            // never cast NULLs, just assign types to them
            return false;
        }

        if (typeName(to) == ANY) {
            // all types can be implicitly interpreted as ANY
            return false;
        }

        if (isParameter(node)) {
            // never cast parameters, just assign types to them
            return false;
        }

        if (isLiteralRemoveMe(node) && !(isTemporal(from) || isTemporal(to) || isChar(from) || isChar(to))) {
            // never cast literals, let Calcite decide on temporal and char ones
            return false;
        }

        if (typeName(from) != typeName(to)) {
            // Force conversion between different types.
            return true;
        }

        // TODO: Check if we need this delegation
        return super.needToCast(scope, node, to);
    }
}

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

package com.hazelcast.sql.impl.calcite.validate.types;

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.NULL;

/**
 * Provides custom coercion strategies supporting {@link HazelcastIntegerType}
 * and assigning more precise types comparing to the standard Calcite coercion.
 */
public final class HazelcastTypeCoercion extends TypeCoercionImpl {
    public HazelcastTypeCoercion(HazelcastSqlValidator validator) {
        super(HazelcastTypeFactory.INSTANCE, validator);
    }

    @Override
    public boolean coerceOperandType(SqlValidatorScope scope, SqlCall call, int index, RelDataType targetType) {
        SqlNode operand = call.getOperandList().get(index);

        // Just update the inferred type if casting is not needed
        if (!requiresCast(scope, operand, targetType)) {
            updateInferredType(operand, targetType);

            return false;
        }

        SqlDataTypeSpec targetTypeSpec;

        if (targetType instanceof HazelcastIntegerType) {
            targetTypeSpec = new SqlDataTypeSpec(
                new HazelcastIntegerTypeNameSpec((HazelcastIntegerType) targetType),
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

    private boolean requiresCast(SqlValidatorScope scope, SqlNode node, RelDataType to) {
        RelDataType from = validator.deriveType(scope, node);

        if (from.getSqlTypeName() == NULL || SqlUtil.isNullLiteral(node, false) || node.getKind() == SqlKind.DYNAMIC_PARAM) {
            // Never cast NULLs or dynamic params, just assign types to them
            return false;
        }

        // CAST is only required between different types.
        return from.getSqlTypeName() != to.getSqlTypeName();

        // This change is incorrect. We add CAST only when type names are
        // different. If type names are equal, but types are different (e.g.
        // BIGINT(63) vs BIGINT(64)), we only update the operand type in the
        // validator. See usages of HazelcastTypeCoercion.coerceOperandType.
//        return !from.equals(to);
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
        // TODO remove
        return super.rowTypeCoercion(scope, query, columnIndex, targetType);
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
        return super.querySourceCoercion(scope, sourceRowType, targetRowType, query);
//        throw new UnsupportedOperationException("Should not be called");
    }
}

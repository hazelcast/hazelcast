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
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.function.Consumer;

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
        return coerceNode(scope, operand, targetType, cast -> call.setOperand(index, cast));
    }

    private boolean coerceNode(
            SqlValidatorScope scope,
            SqlNode node,
            RelDataType targetType,
            Consumer<SqlNode> replaceFn
    ) {
        // Just update the inferred type if casting is not needed
        if (!requiresCast(scope, node, targetType)) {
            updateInferredType(node, targetType);

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

        SqlNode cast = HazelcastSqlOperatorTable.CAST.createCall(SqlParserPos.ZERO, node, targetTypeSpec);

        replaceFn.accept(cast);

        validator.deriveType(scope, cast);

        return true;
    }

    @Override
    protected boolean coerceColumnType(SqlValidatorScope scope, SqlNodeList nodeList, int index, RelDataType targetType) {
        // This will happen when there is a star/dynamic-star column in the select list,
        // and the source is values expression, i.e. `select * from (values(1, 2, 3))`.
        // There is no need to coerce the column type, only remark
        // the inferred row type has changed, we will then add in type coercion
        // when expanding star/dynamic-star.

        // See SqlToRelConverter#convertSelectList for details.
        if (index >= nodeList.getList().size()) {
            // Can only happen when there is a star(*) in the column,
            // just return true.
            return true;
        }

        final SqlNode node = nodeList.get(index);
        if (node instanceof SqlIdentifier) {
            // Do not expand a star/dynamic table col.
            SqlIdentifier node1 = (SqlIdentifier) node;
            if (node1.isStar()) {
                return true;
            } else if (DynamicRecordType.isDynamicStarColName(Util.last(node1.names))) {
                // Should support implicit cast for dynamic table.
                return false;
            }
        }

        if (node instanceof SqlCall) {
            SqlCall node2 = (SqlCall) node;
            if (node2.getOperator().kind == SqlKind.AS) {
                final SqlNode operand = node2.operand(0);
                return coerceNode(scope, operand, targetType, cast -> node2.setOperand(0, cast));
            }
        }

        return coerceNode(scope, node, targetType, cast -> nodeList.set(index, cast));
    }

    private boolean requiresCast(SqlValidatorScope scope, SqlNode node, RelDataType to) {
        RelDataType from = validator.deriveType(scope, node);

        if (from.getSqlTypeName() == NULL || SqlUtil.isNullLiteral(node, false) || node.getKind() == SqlKind.DYNAMIC_PARAM) {
            // Never cast NULLs or dynamic params, just assign types to them
            return false;
        }

        // CAST is only required between different types.
        return from.getSqlTypeName() != to.getSqlTypeName();
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
        // We use the superclass implementation - it only decides, based on the query type, to call
        // either coerceColumnType() or coerceOperandType()
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
        // the code below copied from superclass implementation, but uses our `canCast` method
        final List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
        final List<RelDataTypeField> targetFields = targetRowType.getFieldList();
        final int sourceCount = sourceFields.size();
        for (int i = 0; i < sourceCount; i++) {
            RelDataType sourceType = sourceFields.get(i).getType();
            RelDataType targetType = targetFields.get(i).getType();
            if (!SqlTypeUtil.equalSansNullability(validator.getTypeFactory(), sourceType, targetType)
                    && !HazelcastTypeUtils.canCast(sourceType, targetType)) {
                // Return early if types are not equal and can not do type coercion.
                return false;
            }
        }
        boolean coerced = false;
        for (int i = 0; i < sourceFields.size(); i++) {
            RelDataType targetType = targetFields.get(i).getType();
            coerced = coerceSourceRowType(scope, query, i, targetType) || coerced;
        }
        return coerced;
    }

    // copied from TypeCoercionImpl
    private boolean coerceSourceRowType(
            SqlValidatorScope sourceScope,
            SqlNode query,
            int columnIndex,
            RelDataType targetType) {
        switch (query.getKind()) {
            case INSERT:
                SqlInsert insert = (SqlInsert) query;
                return coerceSourceRowType(sourceScope,
                        insert.getSource(),
                        columnIndex,
                        targetType);
            case UPDATE:
                SqlUpdate update = (SqlUpdate) query;
                if (update.getSourceExpressionList() != null) {
                    final SqlNodeList sourceExpressionList = update.getSourceExpressionList();
                    return coerceColumnType(sourceScope, sourceExpressionList, columnIndex, targetType);
                } else {
                    return coerceSourceRowType(sourceScope,
                            update.getSourceSelect(),
                            columnIndex,
                            targetType);
                }
            default:
                return rowTypeCoercion(sourceScope, query, columnIndex, targetType);
        }
    }
}

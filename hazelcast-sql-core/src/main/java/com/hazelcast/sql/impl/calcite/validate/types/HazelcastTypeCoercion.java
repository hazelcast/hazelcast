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

package com.hazelcast.sql.impl.calcite.validate.types;

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

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
        throw new UnsupportedOperationException("Should not be called");
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

    /**
     * {@inheritDoc}
     *
     * We change the contract of the superclass' return type. According to the
     * superclass contract we're supposed to return true iff we successfully
     * added a CAST. This method returns true if the expression can now be
     * assigned to {@code targetType}, either because a CAST was added, or
     * because it already was assignable (e.g. the type was same). This is
     * needed for {@link #querySourceCoercion} method, which calls this method.
     *
     * @return True, if the source column can now be assigned to {@code
     *      targetType}
     */
    @Override
    public boolean rowTypeCoercion(SqlValidatorScope scope, SqlNode query, int columnIndex, RelDataType targetType) {
        switch (query.getKind()) {
            case SELECT:
                SqlSelect selectNode = (SqlSelect) query;
                SqlValidatorScope scope1 = validator.getSelectScope(selectNode);
                if (!rowTypeElementCoercion(scope1, selectNode.getSelectList().get(columnIndex), targetType,
                        newNode -> selectNode.getSelectList().set(columnIndex, newNode))) {
                    return false;
                }
                updateInferredColumnType(scope1, query, columnIndex, targetType);
                return true;
            case VALUES:
                for (SqlNode rowConstructor : ((SqlCall) query).getOperandList()) {
                    if (!rowTypeElementCoercion(scope, ((SqlCall) rowConstructor).operand(columnIndex), targetType,
                            newNode -> ((SqlCall) rowConstructor).setOperand(columnIndex, newNode))) {
                        return false;
                    }
                }
                updateInferredColumnType(scope, query, columnIndex, targetType);
                return true;
            default:
                throw new UnsupportedOperationException("unexpected: " + query.getKind());
        }
    }

    private boolean rowTypeElementCoercion(
            SqlValidatorScope scope,
            SqlNode rowElement,
            RelDataType targetType,
            Consumer<SqlNode> replaceFn
    ) {
        RelDataType sourceType = validator.deriveType(scope, rowElement);

        QueryDataType sourceHzType = HazelcastTypeUtils.toHazelcastType(sourceType.getSqlTypeName());
        QueryDataType targetHzType = HazelcastTypeUtils.toHazelcastType(targetType.getSqlTypeName());

        if (sourceHzType.getTypeFamily() == targetHzType.getTypeFamily()) {
            // Types are in the same family, do nothing.
            return true;
        }

        boolean valid = sourceAndTargetAreNumeric(targetHzType, sourceHzType)
                || sourceAndTargetAreTemporalAndSourceCanBeConvertedToTarget(targetHzType, sourceHzType)
                || targetIsTemporalAndSourceIsVarcharLiteral(targetHzType, sourceHzType, rowElement);

        if (!valid) {
            // Types cannot be converted to each other, fail to coerce
            return false;
        }

        // Types are in the same group, cast source to target.
        coerceNode(scope, rowElement, targetType, replaceFn);
        return true;
    }

    private static boolean sourceAndTargetAreNumeric(QueryDataType highHZType, QueryDataType lowHZType) {
        return (highHZType.getTypeFamily().isNumeric() && lowHZType.getTypeFamily().isNumeric());
    }

    private static boolean sourceAndTargetAreTemporalAndSourceCanBeConvertedToTarget(QueryDataType targetHzType,
                                                                                     QueryDataType sourceHzType) {
        return targetHzType.getTypeFamily().isTemporal()
                && sourceHzType.getTypeFamily().isTemporal()
                && sourceHzType.getConverter().canConvertTo(targetHzType.getTypeFamily());
    }

    private static boolean targetIsTemporalAndSourceIsVarcharLiteral(QueryDataType targetHzType,
                                                                     QueryDataType sourceHzType, SqlNode sourceNode) {
        return targetHzType.getTypeFamily().isTemporal()
                && sourceHzType.getTypeFamily() == QueryDataTypeFamily.VARCHAR
                && sourceNode instanceof SqlLiteral;
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

    /**
     * {@inheritDoc}
     *
     * We change the contract of the superclass' return type. According to the
     * superclass contract we're supposed to return true iff any coercion
     * happened. This method returns true if the {@code sourceRowType} was
     * changed so that it can now be assigned to the {@code targetRowType},
     * either because a CAST was added, or because it already was assignable
     * (e.g. the type was same), and this must hold for all record fields. The
     * Calcite code that calls this method assumes this.
     *
     * @return True, if the {@code sourceRowType} can now be assigned to {@code
     *      targetRowType}
     */
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
        boolean canAssign = true;
        for (int i = 0; i < sourceFields.size() && canAssign; i++) {
            RelDataType targetType = targetFields.get(i).getType();
            canAssign = coerceSourceRowType(scope, query, i, targetType);
        }
        return canAssign;
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

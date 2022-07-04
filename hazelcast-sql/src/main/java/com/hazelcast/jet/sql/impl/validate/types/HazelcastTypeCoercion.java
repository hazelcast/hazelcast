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

package com.hazelcast.jet.sql.impl.validate.types;

import com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

import java.util.List;
import java.util.function.Consumer;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isNullOrUnknown;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Provides custom coercion strategies supporting {@link HazelcastIntegerType}
 * and assigning more precise types comparing to the standard Calcite coercion.
 */
public final class HazelcastTypeCoercion extends TypeCoercionImpl {

    public HazelcastTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
        super(typeFactory, validator);
    }

    @Override
    public boolean coerceOperandType(SqlValidatorScope scope, SqlCall call, int index, RelDataType targetType) {
        SqlNode operand = call.operand(index);
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
        } else if (targetType.getSqlTypeName() == ANY) {
            // without this the subsequent call to UnsupportedOperationVerifier will fail with "we do not support ANY"
            targetTypeSpec =
                    new SqlDataTypeSpec(new SqlUserDefinedTypeNameSpec("OBJECT", SqlParserPos.ZERO), SqlParserPos.ZERO)
                            .withNullable(targetType.isNullable());
        } else if (targetType.getFamily() == HazelcastJsonType.FAMILY) {
            targetTypeSpec = HazelcastJsonType.TYPE_SPEC;
        } else {
            targetTypeSpec = SqlTypeUtil.convertTypeToSpec(targetType);
        }

        SqlNode cast = cast(node, targetTypeSpec);
        replaceFn.accept(cast);
        validator.deriveType(scope, cast);
        return true;
    }

    private static SqlNode cast(SqlNode node, SqlDataTypeSpec targetTypeSpec) {
        if (node.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
            // transform name => 'value' into name => cast('value')
            SqlBasicCall call = (SqlBasicCall) node;

            SqlNode value = call.getOperandList().get(0);
            SqlNode name = call.getOperandList().get(1);

            SqlNode cast = cast(value, targetTypeSpec);

            return call.getOperator().createCall(SqlParserPos.ZERO, cast, name);
        } else {
            return HazelcastSqlOperatorTable.CAST.createCall(SqlParserPos.ZERO, node, targetTypeSpec);
        }
    }

    private boolean requiresCast(SqlValidatorScope scope, SqlNode node, RelDataType to) {
        RelDataType from = validator.deriveType(scope, node);

        if (isNullOrUnknown(from.getSqlTypeName()) || SqlUtil.isNullLiteral(node, false)
                || node.getKind() == SqlKind.DYNAMIC_PARAM) {
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
     * <p>
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
                SqlValidatorScope selectScope = validator.getSelectScope(selectNode);
                if (!rowTypeElementCoercion(selectScope, selectNode.getSelectList().get(columnIndex), targetType,
                        newNode -> selectNode.getSelectList().set(columnIndex, newNode))) {
                    return false;
                }
                updateInferredColumnType(selectScope, query, columnIndex, targetType);
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

    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    public boolean rowTypeElementCoercion(
            SqlValidatorScope scope,
            SqlNode rowElement,
            RelDataType targetType,
            Consumer<SqlNode> replaceFn
    ) {
        if (targetType.equals(scope.getValidator().getUnknownType())) {
            return false;
        }
        RelDataType sourceType = validator.deriveType(scope, rowElement);

        QueryDataType sourceHzType = HazelcastTypeUtils.toHazelcastType(sourceType);
        QueryDataType targetHzType = HazelcastTypeUtils.toHazelcastType(targetType);

        if (sourceHzType.getTypeFamily() == targetHzType.getTypeFamily() || targetHzType == OBJECT) {
            // Do nothing.
            return true;
        }

        boolean valid = sourceAndTargetAreNumeric(targetHzType, sourceHzType)
                || sourceAndTargetAreTemporalAndSourceCanBeConvertedToTarget(targetHzType, sourceHzType)
                || targetIsTemporalAndSourceIsVarcharLiteral(targetHzType, sourceHzType, rowElement)
                || sourceHzType.getTypeFamily() == QueryDataTypeFamily.NULL
                || sourceHzType.getTypeFamily() == QueryDataTypeFamily.VARCHAR
                        && targetHzType.getTypeFamily() == QueryDataTypeFamily.JSON;

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
        SqlKind sourceKind = sourceNode.getKind();
        if (sourceKind == SqlKind.AS) {
            return targetIsTemporalAndSourceIsVarcharLiteral(
                    targetHzType,
                    sourceHzType,
                    ((SqlBasicCall) sourceNode).operand(0)
            );
        } else {
            return targetHzType.getTypeFamily().isTemporal()
                    && sourceHzType.getTypeFamily() == QueryDataTypeFamily.VARCHAR
                    && sourceKind == SqlKind.LITERAL;
        }
    }

    @Override
    public boolean caseWhenCoercion(SqlCallBinding callBinding) {
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
        assert sourceFields.size() == targetFields.size();
        final int fieldCount = sourceFields.size();
        for (int i = 0; i < fieldCount; i++) {
            RelDataType sourceType = sourceFields.get(i).getType();
            RelDataType targetType = targetFields.get(i).getType();
            if (!SqlTypeUtil.equalSansNullability(validator.getTypeFactory(), sourceType, targetType)
                    && !HazelcastTypeUtils.canCast(sourceType, targetType)
                    || !coerceSourceRowType(scope, query, i, fieldCount, targetType)) {
                SqlNode node = getNthExpr(query, i, fieldCount);
                throw scope.getValidator().newValidationError(node,
                        RESOURCE.typeNotAssignable(
                                targetFields.get(i).getName(), targetType.toString(),
                                sourceFields.get(i).getName(), sourceType.toString()));
            }
        }

        // We always return true to defuse the fallback mechanism in the caller.
        // Instead, we throw the validation error ourselves above if we can't assign.
        return true;
    }

    /**
     * Copied from {@code org.apache.calcite.sql.validate.SqlValidatorImpl#getNthExpr()}.
     * <p>
     * Locates the n-th expression in an INSERT or UPDATE query.
     *
     * @param query       Query
     * @param ordinal     Ordinal of expression
     * @param sourceCount Number of expressions
     * @return Ordinal'th expression, never null
     */
    private SqlNode getNthExpr(SqlNode query, int ordinal, int sourceCount) {
        if (query instanceof SqlInsert) {
            SqlInsert insert = (SqlInsert) query;
            if (insert.getTargetColumnList() != null) {
                return insert.getTargetColumnList().get(ordinal);
            } else {
                return getNthExpr(
                        insert.getSource(),
                        ordinal,
                        sourceCount);
            }
        } else if (query instanceof SqlUpdate) {
            // trailing elements of selectList are equal to elements of sourceExpressionList
            // see JetSqlValidator.validateUpdate()
            SqlUpdate update = (SqlUpdate) query;
            SqlNodeList selectList = update.getSourceSelect().getSelectList();
            return selectList.get(selectList.size() - sourceCount + ordinal);
        } else if (query instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) query;
            if (select.getSelectList().size() == sourceCount) {
                return select.getSelectList().get(ordinal);
            } else {
                // give up
                return query;
            }
        } else {
            // give up
            return query;
        }
    }

    // originally copied from TypeCoercionImpl
    private boolean coerceSourceRowType(
            SqlValidatorScope sourceScope,
            SqlNode query,
            int columnIndex,
            int totalColumns,
            RelDataType targetType
    ) {
        switch (query.getKind()) {
            case INSERT:
                SqlInsert insert = (SqlInsert) query;
                return coerceSourceRowType(sourceScope,
                        insert.getSource(),
                        columnIndex,
                        totalColumns,
                        targetType);
            case UPDATE:
                // trailing elements of selectList are equal to elements of sourceExpressionList
                // see JetSqlValidator.validateUpdate()
                SqlUpdate update = (SqlUpdate) query;
                SqlNodeList selectList = update.getSourceSelect().getSelectList();
                return coerceSourceRowType(sourceScope, selectList, selectList.size() - totalColumns + columnIndex, targetType);
            default:
                return rowTypeCoercion(sourceScope, query, columnIndex, targetType);
        }
    }

    private boolean coerceSourceRowType(SqlValidatorScope scope, SqlNodeList nodeList, int index, RelDataType targetType) {
        SqlNode node = nodeList.get(index);
        return rowTypeElementCoercion(scope, node, targetType, cast -> nodeList.set(index, cast));
    }
}

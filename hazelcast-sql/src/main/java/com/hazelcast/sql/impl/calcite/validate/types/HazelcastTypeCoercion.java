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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil.isLiteral;
import static com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil.isParameter;
import static com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil.numericValue;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.canCast;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isChar;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isFloatingPoint;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isInteger;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isNumeric;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isTemporal;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.typeName;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.withHigherPrecedence;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.withHigherPrecedenceForLiterals;
import static org.apache.calcite.sql.SqlKind.BETWEEN;
import static org.apache.calcite.sql.SqlKind.BINARY_ARITHMETIC;
import static org.apache.calcite.sql.SqlKind.BINARY_COMPARISON;
import static org.apache.calcite.sql.SqlKind.BINARY_EQUALITY;
import static org.apache.calcite.sql.SqlKind.MINUS_PREFIX;
import static org.apache.calcite.sql.SqlKind.PLUS_PREFIX;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
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
        SqlKind kind = binding.getOperator().getKind();
        if (!kind.belongsTo(BINARY_ARITHMETIC) && kind != PLUS_PREFIX && kind != MINUS_PREFIX) {
            return super.binaryArithmeticCoercion(binding);
        }

        // Infer types.

        RelDataType[] types = inferTypes(binding.getScope(), binding.operands(), true);
        if (types == null) {
            return false;
        }

        // Do the coercion.

        boolean coerced = false;
        for (int i = 0; i < types.length - 1; ++i) {
            boolean operandCoerced = coerceOperandType(binding.getScope(), binding.getCall(), i, types[i]);
            coerced |= operandCoerced;
        }

        return coerced;
    }

    @Override
    public boolean binaryComparisonCoercion(SqlCallBinding binding) {
        SqlKind kind = binding.getOperator().getKind();
        if (!kind.belongsTo(BINARY_EQUALITY) && !kind.belongsTo(BINARY_COMPARISON) && kind != BETWEEN) {
            return super.binaryComparisonCoercion(binding);
        }

        // Infer types.

        RelDataType[] types = inferTypes(binding.getScope(), binding.operands(), false);
        if (types == null) {
            return false;
        }

        // Do the coercion.

        RelDataType commonType = types[types.length - 1];

        boolean coerced = false;
        for (int i = 0; i < types.length - 1; ++i) {
            RelDataType type = types[i];
            type = TYPE_FACTORY.createTypeWithNullability(commonType, type.isNullable());
            boolean operandCoerced = coerceOperandType(binding.getScope(), binding.getCall(), i, type);
            coerced |= operandCoerced;

            // If the operand was coerced to integer type, reassign its CAST type
            // back to the common type: '0':VARCHAR -> CAST('0' AS INT(31)):INT(0)
            // -> CAST('0' AS INT(31)):INT(31).
            if (operandCoerced && isInteger(type)) {
                updateInferredType(binding.operand(i), type);
            }
        }

        return coerced;
    }

    @Override
    public boolean caseWhenCoercion(SqlCallBinding binding) {
        // Infer types.

        SqlCase call = (SqlCase) binding.getCall();
        SqlNodeList thenOperands = call.getThenOperands();

        List<SqlNode> operands = new ArrayList<>(thenOperands.size() + 1);
        operands.addAll(thenOperands.getList());
        operands.add(call.getElseOperand());

        RelDataType[] types = inferTypes(binding.getScope(), operands, false);
        if (types == null) {
            return false;
        }

        // Do the coercion.

        boolean coerced = false;
        for (int i = 0; i < operands.size() - 1; ++i) {
            coerced |= coerceElementType(binding.getScope(), thenOperands, i, types[i]);
        }
        coerced |= coerceOperandType(binding.getScope(), call, 3, types[operands.size() - 1]);

        updateInferredType(call, types[types.length - 1]);

        return coerced;
    }

    @Override
    public RelDataType implicitCast(RelDataType in, SqlTypeFamily expected) {
        // enables implicit conversion from CHAR to BOOLEAN
        if (CHAR_TYPES.contains(typeName(in)) && expected == SqlTypeFamily.BOOLEAN) {
            return TYPE_FACTORY.createSqlType(BOOLEAN, in.isNullable());
        }

        return super.implicitCast(in, expected);
    }

    @Override
    protected void updateInferredType(SqlNode node, RelDataType type) {
        ((HazelcastSqlValidator) validator).setKnownNodeType(node, type);
        super.updateInferredType(node, type);
    }

    @Override
    protected boolean coerceOperandType(SqlValidatorScope scope, SqlCall call, int index, RelDataType to) {
        SqlNode operand = call.getOperandList().get(index);
        RelDataType from = validator.deriveType(scope, operand);

        // Just update the inferred type if casting is not needed. But if casting
        // is not possible, still insert the cast to fail on its validation later.
        if (!needToCast(scope, operand, to) && canCast(from, to)) {
            updateInferredType(operand, to);
            return false;
        }

        SqlNode cast = makeCast(operand, to);
        call.setOperand(index, cast);
        // derive the type of the newly created CAST immediately
        validator.deriveType(scope, cast);
        return true;
    }

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
        if (typeName(from) == ANY) {
            // casting from ANY is always required
            return true;
        }

        if (isParameter(node)) {
            // never cast parameters, just assign types to them
            return false;
        }

        if (isLiteral(node) && !(isTemporal(from) || isTemporal(to) || isChar(from) || isChar(to))) {
            // never cast literals, let Calcite decide on temporal and char ones
            return false;
        }

        return super.needToCast(scope, node, to);
    }

    private boolean coerceElementType(SqlValidatorScope scope, SqlNodeList list, int index, RelDataType to) {
        SqlNode element = list.get(index);
        RelDataType from = validator.deriveType(scope, element);

        // Just update the inferred type if casting is not needed. But if casting
        // is not possible, still insert the cast to fail later on its validation.
        if (!needToCast(scope, element, to) && canCast(from, to)) {
            updateInferredType(element, to);
            return false;
        }

        SqlNode cast = makeCast(element, to);
        list.set(index, cast);
        // derive the type of the newly created CAST immediately
        validator.deriveType(scope, cast);
        return true;
    }

    private static SqlNode makeCast(SqlNode node, RelDataType type) {
        return HazelcastSqlOperatorTable.CAST.createCall(SqlParserPos.ZERO, node, SqlTypeUtil.convertTypeToSpec(type));
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength", "checkstyle:NPathComplexity",
            "checkstyle:NestedIfDepth"})
    private RelDataType[] inferTypes(SqlValidatorScope scope, List<SqlNode> operands, boolean assumeNumeric) {
        // Infer common type from columns and sub-expressions.

        RelDataType commonType = null;
        boolean seenParameters = false;
        boolean seenChar = false;

        for (SqlNode operand : operands) {
            RelDataType operandType = validator.deriveType(scope, operand);
            if (isLiteral(operand)) {
                continue;
            }

            if (isParameter(operand)) {
                seenParameters = true;
            } else {
                commonType = commonType == null ? operandType : withHigherPrecedence(operandType, commonType);
                seenChar |= isChar(operandType);
            }
        }

        // Continue common type inference on numeric literals.

        for (SqlNode operand : operands) {
            RelDataType operandType = validator.deriveType(scope, operand);
            if (!isLiteral(operand) || !isNumeric(operandType)) {
                continue;
            }
            SqlLiteral literal = (SqlLiteral) operand;

            if (literal.getValue() == null) {
                operandType = TYPE_FACTORY.createSqlType(NULL);
            } else {
                BigDecimal numeric = literal.getValueAs(BigDecimal.class);
                operandType = narrowestTypeFor(numeric, commonType == null ? null : typeName(commonType));
            }

            commonType = commonType == null ? operandType : withHigherPrecedenceForLiterals(operandType, commonType);
        }

        // Continue common type inference on non-numeric literals.

        for (SqlNode operand : operands) {
            RelDataType operandType = validator.deriveType(scope, operand);
            if (!isLiteral(operand) || isNumeric(operandType)) {
                continue;
            }
            SqlLiteral literal = (SqlLiteral) operand;

            if (literal.getValue() == null) {
                operandType = TYPE_FACTORY.createSqlType(NULL);
            } else if (isChar(operandType) && (commonType != null && isNumeric(commonType) || assumeNumeric)) {
                // Infer proper numeric type for char literals.

                BigDecimal numeric = numericValue(operand);
                assert numeric != null;
                operandType = narrowestTypeFor(numeric, commonType == null ? null : typeName(commonType));
            }

            commonType = commonType == null ? operandType : withHigherPrecedenceForLiterals(operandType, commonType);
        }

        // seen only parameters
        if (commonType == null) {
            assert seenParameters;
            return null;
        }

        // can't infer parameter types if seen only NULLs
        if (typeName(commonType) == NULL && seenParameters) {
            return null;
        }

        // fallback to DOUBLE from CHAR, if numeric types assumed
        if (isChar(commonType) && assumeNumeric) {
            commonType = TYPE_FACTORY.createSqlType(DOUBLE);
        }

        // widen integer common type: ? + 1 -> BIGINT instead of TINYINT
        if ((seenParameters || seenChar) && isInteger(commonType)) {
            commonType = TYPE_FACTORY.createSqlType(BIGINT);
        }

        // Assign final types to everything based on the inferred common type.

        RelDataType[] types = new RelDataType[operands.size() + 1];
        boolean nullable = false;
        for (int i = 0; i < operands.size(); ++i) {
            SqlNode operand = operands.get(i);
            RelDataType operandType = validator.deriveType(scope, operand);

            if (isParameter(operand)) {
                // Just assign the common type to parameters.

                types[i] = TYPE_FACTORY.createTypeWithNullability(commonType, true);
                nullable = true;
            } else if (isLiteral(operand)) {
                SqlLiteral literal = (SqlLiteral) operand;

                if (literal.getValue() == null) {
                    // Just assign the common type to NULLs.

                    types[i] = TYPE_FACTORY.createTypeWithNullability(commonType, true);
                    nullable = true;
                } else if (isNumeric(operandType) || (isChar(operandType) && isNumeric(commonType))) {
                    // Assign final numeric types to numeric and char literals.

                    RelDataType literalType;
                    BigDecimal numeric = numericValue(operand);
                    assert numeric != null;
                    if (typeName(commonType) == DECIMAL) {
                        // always enforce DECIMAL interpretation if common type is DECIMAL
                        literalType = TYPE_FACTORY.createSqlType(DECIMAL);
                    } else {
                        literalType = narrowestTypeFor(numeric, typeName(commonType));
                        if (assumeNumeric && isFloatingPoint(commonType)) {
                            // directly use floating-point representation in numeric contexts
                            literalType = withHigherPrecedence(literalType, commonType);
                            literalType = TYPE_FACTORY.createTypeWithNullability(literalType, false);
                        }
                    }
                    types[i] = literalType;
                } else if (isChar(operandType) && !isChar(commonType) && typeName(commonType) != ANY) {
                    // If common type is non-numeric, just assign it to char literals.

                    types[i] = TYPE_FACTORY.createTypeWithNullability(commonType, false);
                } else {
                    // All other literal types.

                    types[i] = operandType;
                    nullable |= typeName(operandType) == NULL;
                }
            } else {
                // Columns and sub-expressions.

                RelDataType type;
                if (isNumeric(operandType) && typeName(commonType) == DECIMAL) {
                    // always enforce cast to DECIMAL if common type is DECIMAL
                    type = commonType;
                } else if (isChar(operandType) && typeName(commonType) != ANY) {
                    // cast char to common type
                    type = commonType;
                } else {
                    type = operandType;
                }
                types[i] = TYPE_FACTORY.createTypeWithNullability(type, operandType.isNullable());
                nullable |= operandType.isNullable();
            }
        }

        commonType = TYPE_FACTORY.createTypeWithNullability(commonType, nullable);
        types[types.length - 1] = commonType;

        return types;
    }

}

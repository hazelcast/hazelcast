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
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

import java.math.BigDecimal;

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
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;

public final class HazelcastTypeCoercion extends TypeCoercionImpl {

    private static final HazelcastTypeFactory TYPE_FACTORY = HazelcastTypeFactory.INSTANCE;

    public HazelcastTypeCoercion(HazelcastSqlValidator validator) {
        super(TYPE_FACTORY, validator);
    }

    @Override
    public boolean binaryArithmeticCoercion(SqlCallBinding binding) {
        SqlKind operatorKind = binding.getCall().getOperator().getKind();

        if (operatorKind.belongsTo(SqlKind.BINARY_ARITHMETIC)) {
            return binaryArithmetic(binding);
        }

        if (binding.getOperandCount() == 1) {
            return unaryArithmetic(binding);
        }

        return super.binaryArithmeticCoercion(binding);
    }

    @Override
    public boolean binaryComparisonCoercion(SqlCallBinding binding) {
        SqlNode lhs = binding.operand(0);
        SqlNode rhs = binding.operand(1);

        if (isParameter(lhs) && isParameter(rhs)) {
            return false;
        }

        RelDataType lhsType = typeOf(lhs, binding);
        RelDataType rhsType = typeOf(rhs, binding);

        if (isParameter(lhs)) {
            lhsType = TYPE_FACTORY.createTypeWithNullability(rhsType, true);
        }
        if (isParameter(rhs)) {
            rhsType = TYPE_FACTORY.createTypeWithNullability(lhsType, true);
        }

        if (typeName(lhsType) == NULL || typeName(rhsType) == NULL) {
            return false;
        }

        RelDataType coerceTo = withHigherPrecedence(lhsType, rhsType);
        if (isNumeric(coerceTo)) {
            if (typeName(lhsType) == BOOLEAN || typeName(rhsType) == BOOLEAN) {
                return false;
            }

            numericBinaryArithmetic(binding, lhs, rhs, lhsType, rhsType);

            lhsType = binding.getOperandType(0);
            rhsType = binding.getOperandType(1);
            coerceTo = withHigherPrecedence(lhsType, rhsType);
        }

        lhsType = TYPE_FACTORY.createTypeWithNullability(coerceTo, lhsType.isNullable());
        rhsType = TYPE_FACTORY.createTypeWithNullability(coerceTo, rhsType.isNullable());

        boolean coerced = coerceOperandType(binding.getScope(), binding.getCall(), 0, lhsType);
        coerced |= coerceOperandType(binding.getScope(), binding.getCall(), 1, rhsType);
        return coerced;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength", "checkstyle:NPathComplexity"})
    @Override
    public boolean caseWhenCoercion(SqlCallBinding binding) {
        SqlCase call = (SqlCase) binding.getCall();
        SqlNodeList thenOperands = call.getThenOperands();

        SqlNode[] operands = new SqlNode[thenOperands.size() + 1];
        for (int i = 0; i < operands.length - 1; ++i) {
            operands[i] = thenOperands.get(i);
        }
        operands[operands.length - 1] = call.getElseOperand();

        // Infer return type from columns and sub-expressions.

        RelDataType returnType = null;
        boolean seenParameters = false;
        boolean seenChar = false;

        for (SqlNode operand : operands) {
            RelDataType operandType = validator.deriveType(binding.getScope(), operand);
            if (isLiteral(operand)) {
                continue;
            }

            if (isParameter(operand)) {
                seenParameters = true;
            } else {
                returnType = returnType == null ? operandType : withHigherPrecedence(operandType, returnType);
                seenChar |= isChar(operandType);
            }
        }

        // Continue inference on numeric literals.

        for (SqlNode operand : operands) {
            RelDataType operandType = validator.deriveType(binding.getScope(), operand);
            if (!isLiteral(operand) || !isNumeric(operandType)) {
                continue;
            }

            BigDecimal numeric = ((SqlLiteral) operand).getValueAs(BigDecimal.class);
            RelDataType literalType = narrowestTypeFor(numeric, returnType == null ? null : typeName(returnType));
            returnType = returnType == null ? literalType : withHigherPrecedenceForLiterals(literalType, returnType);
        }

        // Continue inference on non-numeric literals.

        for (SqlNode operand : operands) {
            RelDataType operandType = validator.deriveType(binding.getScope(), operand);
            if (!isLiteral(operand) || isNumeric(operandType)) {
                continue;
            }

            if (isChar(operandType) && returnType != null && isNumeric(returnType)) {
                // Infer proper numeric type for char literals.

                BigDecimal numeric = numericValue(operand);
                assert numeric != null;
                RelDataType literalType = narrowestTypeFor(numeric, typeName(returnType));
                returnType = withHigherPrecedenceForLiterals(literalType, returnType);
            } else {
                returnType = returnType == null ? operandType : withHigherPrecedence(operandType, returnType);
            }
        }

        // seen only parameters
        if (returnType == null) {
            assert seenParameters;
            return false;
        }

        // can't infer parameter types if seen only NULLs
        if (typeName(returnType) == NULL && seenParameters) {
            return false;
        }

        // widen integer return type: then ? ... then 1 -> BIGINT instead of TINYINT
        if ((seenParameters || seenChar) && isInteger(returnType)) {
            returnType = TYPE_FACTORY.createSqlType(BIGINT);
        }

        // Assign final types to everything based on the inferred return type.

        RelDataType[] types = new RelDataType[operands.length];
        boolean nullable = false;
        for (int i = 0; i < operands.length; ++i) {
            SqlNode operand = operands[i];
            RelDataType operandType = validator.deriveType(binding.getScope(), operand);

            if (isParameter(operand)) {
                types[i] = TYPE_FACTORY.createTypeWithNullability(returnType, true);
                nullable = true;
            } else if (isLiteral(operand)) {
                if (isNumeric(operandType) || (isChar(operandType) && isNumeric(returnType))) {
                    // Assign final numeric types to numeric and char literals.

                    BigDecimal numeric = numericValue(operand);
                    assert numeric != null;
                    RelDataType literalType;
                    if (typeName(returnType) == DECIMAL) {
                        // enforce DECIMAL interpretation if return type is DECIMAL
                        literalType = TYPE_FACTORY.createSqlType(DECIMAL);
                    } else {
                        literalType = narrowestTypeFor(numeric, typeName(returnType));
                    }
                    types[i] = literalType;
                } else if (isChar(operandType) && !isChar(returnType) && typeName(returnType) != ANY) {
                    // Assign non-numeric types to char literals.

                    types[i] = TYPE_FACTORY.createTypeWithNullability(returnType, false);
                } else {
                    // All other literal types.

                    types[i] = operandType;
                    nullable |= typeName(operandType) == NULL;
                }
            } else {
                // Columns and sub-expressions.

                RelDataType type;
                if (isNumeric(operandType) && typeName(returnType) == DECIMAL) {
                    // enforce cast to DECIMAL if return type is DECIMAL
                    type = returnType;
                } else if (isChar(operandType) && typeName(returnType) != ANY) {
                    // cast char to return type
                    type = returnType;
                } else {
                    type = operandType;
                }
                types[i] = TYPE_FACTORY.createTypeWithNullability(type, operandType.isNullable());
                nullable |= operandType.isNullable();
            }
        }

        // Finally do the coercion.

        boolean coerced = false;
        for (int i = 0; i < operands.length - 1; ++i) {
            coerced |= coerceNodeListElementType(binding.getScope(), thenOperands, i, types[i]);
        }
        coerced |= coerceOperandType(binding.getScope(), call, 3, types[types.length - 1]);

        returnType = TYPE_FACTORY.createTypeWithNullability(returnType, nullable);
        updateInferredType(call, returnType);

        return coerced;
    }

    @Override
    public RelDataType implicitCast(RelDataType in, SqlTypeFamily expected) {
        if (in.getSqlTypeName() == NULL) {
            return in;
        }

        if (CHAR_TYPES.contains(in.getSqlTypeName()) && expected == SqlTypeFamily.BOOLEAN) {
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

        // just update the inferred type if casting is not needed
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

    @Override
    protected boolean needToCast(SqlValidatorScope scope, SqlNode node, RelDataType to) {
        RelDataType from = validator.deriveType(scope, node);

        if (typeName(from) == typeName(to)) {
            // already of the same type
            return false;
        }

        if (typeName(to) == ANY) {
            // all types can be implicitly casted to ANY
            return false;
        }
        if (typeName(from) == ANY) {
            // casting from ANY is always required
            return true;
        }

        if (isParameter(node)) {
            // never cast parameters
            return false;
        }

        if (isLiteral(node) && !(isTemporal(from) || isTemporal(to) || isChar(from))) {
            // never cast literals, let Calcite decide on temporal and char ones
            return false;
        }

        return super.needToCast(scope, node, to);
    }

    private boolean coerceNodeListElementType(SqlValidatorScope scope, SqlNodeList list, int index, RelDataType to) {
        SqlNode element = list.get(index);
        RelDataType from = validator.deriveType(scope, element);

        // just update the inferred type if casting is not needed
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

    private boolean binaryArithmetic(SqlCallBinding binding) {
        SqlNode lhs = binding.operand(0);
        SqlNode rhs = binding.operand(1);

        RelDataType lhsType = typeOf(lhs, binding);
        RelDataType rhsType = typeOf(rhs, binding);

        // Trim some trivial cases.

        if (isParameter(lhs) && isParameter(rhs)) {
            return false;
        }

        if (typeName(lhsType) == ANY || typeName(rhsType) == ANY) {
            return false;
        }

        if (typeName(lhsType) == BOOLEAN || typeName(rhsType) == BOOLEAN) {
            return false;
        }

        if (typeName(lhsType) == NULL && typeName(rhsType) == NULL) {
            return false;
        }

        // Infer types.

        if (isTemporal(lhsType) || isTemporal(rhsType)) {
            // trust Calcite on temporal types
            return super.binaryArithmeticCoercion(binding);
        } else {
            return numericBinaryArithmetic(binding, lhs, rhs, lhsType, rhsType);
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private boolean numericBinaryArithmetic(SqlCallBinding binding, SqlNode lhs, SqlNode rhs, RelDataType lhsType,
                                            RelDataType rhsType) {
        // Convert literals to their numeric values.

        BigDecimal lhsNumeric = numericValue(lhs);
        BigDecimal rhsNumeric = numericValue(rhs);

        // All non-NULL literals now have numeric values: assign types to literals.

        if (lhsNumeric != null) {
            lhsType = narrowestTypeFor(lhsNumeric, isLiteral(rhs) ? null : typeName(rhsType));
        }
        if (rhsNumeric != null) {
            rhsType = narrowestTypeFor(rhsNumeric, isLiteral(lhs) ? null : typeName(lhsType));
        }

        if (isLiteral(lhs) && isFloatingPoint(rhsType) || isLiteral(rhs) && isFloatingPoint(lhsType)) {
            RelDataType type = withHigherPrecedence(lhsType, rhsType);

            if (lhsNumeric != null) {
                lhsType = TYPE_FACTORY.createTypeWithNullability(type, lhsType.isNullable());
            }
            if (rhsNumeric != null) {
                rhsType = TYPE_FACTORY.createTypeWithNullability(type, rhsType.isNullable());
            }
        }

        // All non-NULL literals now have numeric types assigned and their
        // values are valid: work on columns, NULLs and parameters.

        if (isParameterOrChar(lhs, lhsType) && isParameterOrChar(rhs, rhsType)) {
            lhsType = TYPE_FACTORY.createSqlType(DOUBLE, lhsType.isNullable());
            rhsType = TYPE_FACTORY.createSqlType(DOUBLE, rhsType.isNullable());
        } else if (isParameterOrChar(lhs, lhsType)) {
            RelDataType type;
            if (isInteger(rhsType)) {
                type = TYPE_FACTORY.createSqlType(BIGINT);
            } else if (typeName(rhsType) == NULL) {
                type = TYPE_FACTORY.createSqlType(DOUBLE);
            } else {
                type = rhsType;
            }
            lhsType = TYPE_FACTORY.createTypeWithNullability(type, isParameter(lhs) || lhsType.isNullable());
        } else if (isParameterOrChar(rhs, rhsType)) {
            RelDataType type;
            if (isInteger(lhsType)) {
                type = TYPE_FACTORY.createSqlType(BIGINT);
            } else if (typeName(lhsType) == NULL) {
                type = TYPE_FACTORY.createSqlType(DOUBLE);
            } else {
                type = lhsType;
            }
            rhsType = TYPE_FACTORY.createTypeWithNullability(type, isParameter(rhs) || rhsType.isNullable());
        }

        if (typeName(rhsType) == DECIMAL && isInteger(lhsType)) {
            lhsType = TYPE_FACTORY.createTypeWithNullability(rhsType, lhsType.isNullable());
        } else if (isInteger(rhsType) && typeName(lhsType) == DECIMAL) {
            rhsType = TYPE_FACTORY.createTypeWithNullability(lhsType, rhsType.isNullable());
        }

        // Finally store the inferred types.

        boolean coerced = coerceOperandType(binding.getScope(), binding.getCall(), 0, lhsType);
        coerced |= coerceOperandType(binding.getScope(), binding.getCall(), 1, rhsType);
        return coerced;
    }

    private boolean unaryArithmetic(SqlCallBinding binding) {
        SqlNode operand = binding.operand(0);
        RelDataType type = validator.deriveType(binding.getScope(), operand);

        if (!isChar(type)) {
            return super.binaryArithmeticCoercion(binding);
        }

        switch (binding.getOperator().getKind()) {
            case PLUS_PREFIX:
            case MINUS_PREFIX:
                BigDecimal numeric = numericValue(operand);
                if (numeric == null) {
                    type = TYPE_FACTORY.createSqlType(DOUBLE, type.isNullable());
                } else {
                    type = narrowestTypeFor(numeric, null);
                }
                return coerceOperandType(binding.getScope(), binding.getCall(), 0, type);

            default:
                // do nothing
        }

        return super.binaryArithmeticCoercion(binding);
    }

    private static boolean isParameterOrChar(SqlNode node, RelDataType type) {
        return isParameter(node) || isChar(type);
    }

    private static RelDataType typeOf(SqlNode node, SqlCallBinding binding) {
        if (isParameter(node)) {
            return binding.getValidator().getUnknownType();
        }

        return binding.getValidator().deriveType(binding.getScope(), node);
    }

}

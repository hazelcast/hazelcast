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

package com.hazelcast.sql.impl.calcite.validate.operators.predicate;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastBinaryOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.param.NumericPrecedenceParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference.wrap;

public final class HazelcastComparisonPredicate extends HazelcastBinaryOperator {

    public static final HazelcastComparisonPredicate EQUALS = new HazelcastComparisonPredicate(
        SqlStdOperatorTable.EQUALS
    );

    public static final HazelcastComparisonPredicate NOT_EQUALS = new HazelcastComparisonPredicate(
        SqlStdOperatorTable.NOT_EQUALS
    );

    public static final HazelcastComparisonPredicate GREATER_THAN = new HazelcastComparisonPredicate(
        SqlStdOperatorTable.GREATER_THAN
    );

    public static final HazelcastComparisonPredicate GREATER_THAN_OR_EQUAL = new HazelcastComparisonPredicate(
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
    );

    public static final HazelcastComparisonPredicate LESS_THAN = new HazelcastComparisonPredicate(
        SqlStdOperatorTable.LESS_THAN
    );

    public static final HazelcastComparisonPredicate LESS_THAN_OR_EQUAL = new HazelcastComparisonPredicate(
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL
    );

    private HazelcastComparisonPredicate(SqlBinaryOperator base) {
        super(
            base.getName(),
            base.getKind(),
            base.getLeftPrec(),
            true,
            wrap(ReturnTypes.BOOLEAN_NULLABLE),
            new OperandTypeInference()
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        SqlNode first = binding.operand(0);
        SqlNode second = binding.operand(1);

        HazelcastSqlValidator validator = binding.getValidator();

        RelDataType firstType = validator.deriveType(binding.getScope(), first);
        RelDataType secondType = validator.deriveType(binding.getScope(), second);

        assert firstType.getSqlTypeName() != SqlTypeName.NULL;
        assert secondType.getSqlTypeName() != SqlTypeName.NULL;

        return checkOperandTypes(binding, throwOnFailure, validator, first, firstType, second, secondType);
    }

    private boolean checkOperandTypes(
        HazelcastCallBinding callBinding,
        boolean throwOnFailure,
        HazelcastSqlValidator validator,
        SqlNode first,
        RelDataType firstType,
        SqlNode second,
        RelDataType secondType
    ) {
        RelDataType winningType = HazelcastTypeSystem.withHigherPrecedence(firstType, secondType);

        if (winningType == firstType) {
            return checkOperandTypesWithPrecedence(
                callBinding,
                throwOnFailure,
                validator,
                first,
                firstType,
                second,
                secondType,
                1
            );
        } else {
            assert winningType == secondType;

            return checkOperandTypesWithPrecedence(
                callBinding,
                throwOnFailure,
                validator,
                second,
                secondType,
                first,
                firstType,
                0
            );
        }
    }

    private boolean checkOperandTypesWithPrecedence(
        HazelcastCallBinding callBinding,
        boolean throwOnFailure,
        HazelcastSqlValidator validator,
        SqlNode high,
        RelDataType highType,
        SqlNode low,
        RelDataType lowType,
        int lowIndex
    ) {
        QueryDataType highHZType = SqlToQueryType.map(highType.getSqlTypeName());
        QueryDataType lowHZType = SqlToQueryType.map(lowType.getSqlTypeName());

        if (highHZType.getTypeFamily().isTemporal() || highHZType.getTypeFamily() == QueryDataTypeFamily.OBJECT) {
            // Disallow comparisons for temporal and OBJECT types.
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            } else {
                return false;
            }
        }

        if (highHZType.getTypeFamily().isNumeric()) {
            // Set flexible parameter converter that allows TINYINT/SMALLINT/INTEGER -> BIGINT conversions
            setNumericParameterConverter(validator, high, highHZType);
            setNumericParameterConverter(validator, low, highHZType);
        }

        if (highHZType.getTypeFamily() == lowHZType.getTypeFamily()) {
            // Types are in the same family, do nothing.
            return true;
        }

        if (highHZType.getTypeFamily() != lowHZType.getTypeFamily()
            && !(highHZType.getTypeFamily().isNumeric() && lowHZType.getTypeFamily().isNumeric())) {
            // Types cannot be converted to each other, throw.
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            } else {
                return false;
            }
        }

        // Types are in the same group, cast lower to higher.
        validator.getTypeCoercion().coerceOperandType(callBinding.getScope(), callBinding.getCall(), lowIndex, highType);

        return true;
    }

    private static void setNumericParameterConverter(HazelcastSqlValidator validator, SqlNode node, QueryDataType type) {
        if (node.getKind() == SqlKind.DYNAMIC_PARAM) {
            SqlDynamicParam node0 = (SqlDynamicParam) node;

            ParameterConverter converter = new NumericPrecedenceParameterConverter(
                node0.getIndex(),
                node.getParserPosition(),
                type
            );

            validator.setParameterConverter(node0.getIndex(), converter);
        }
    }

    private static final class OperandTypeInference implements SqlOperandTypeInference {
        @Override
        public void inferOperandTypes(SqlCallBinding binding, RelDataType returnType, RelDataType[] operandTypes) {
            // Check if we have parameters. If yes, we will upcast integer literals to BIGINT as explained below
            boolean hasParameters = binding.operands().stream().anyMatch(SqlNodeUtil::isParameter);

            int unknownTypeOperandIndex = -1;
            RelDataType knownType = null;

            for (int i = 0; i < binding.getOperandCount(); i++) {
                RelDataType operandType = binding.getOperandType(i);

                if (operandType.getSqlTypeName() == SqlTypeName.NULL) {
                    // Will resolve operand type at this index later.
                    unknownTypeOperandIndex = i;
                } else {
                    if (hasParameters && SqlNodeUtil.isNumericInteger(operandType)) {
                        // If we are here, there is a parameter, and an exact numeric literal.
                        // We upcast the type of the numeric literal to BIGINT, so that an expression `1 > ?` is resolved to
                        // `(BIGINT)1 > (BIGINT)?` rather than `(TINYINT)1 > (TINYINT)?`
                        RelDataType newOperandType = SqlNodeUtil.createType(
                            binding.getTypeFactory(),
                            SqlTypeName.BIGINT,
                            operandType.isNullable()
                        );

                        operandType = newOperandType;
                    }

                    operandTypes[i] = operandType;

                    if (knownType == null) {
                        knownType = operandType;
                    }
                }
            }

            // If we have [UNKNOWN, UNKNOWN] operands, throw an signature error, since we cannot deduce the return type
            if (knownType == null) {
                throw new HazelcastCallBinding(binding).newValidationSignatureError();
            }

            // If there is an operand with an unresolved type, set it to the known type.
            if (unknownTypeOperandIndex != -1) {
                operandTypes[unknownTypeOperandIndex] = knownType;
            }
        }
    }
}

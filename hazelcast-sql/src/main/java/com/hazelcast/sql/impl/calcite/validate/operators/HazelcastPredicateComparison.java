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

package com.hazelcast.sql.impl.calcite.validate.operators;

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.literal.HazelcastSqlLiteral;
import com.hazelcast.sql.impl.calcite.literal.HazelcastSqlLiteralFunction;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil.isParameter;

public final class HazelcastPredicateComparison extends SqlBinaryOperator {
    public static final HazelcastPredicateComparison EQUALS = new HazelcastPredicateComparison(
        SqlStdOperatorTable.EQUALS
    );

    public static final HazelcastPredicateComparison NOT_EQUALS = new HazelcastPredicateComparison(
        SqlStdOperatorTable.NOT_EQUALS
    );

    public static final HazelcastPredicateComparison GREATER_THAN = new HazelcastPredicateComparison(
        SqlStdOperatorTable.GREATER_THAN
    );

    public static final HazelcastPredicateComparison GREATER_THAN_OR_EQUAL = new HazelcastPredicateComparison(
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
    );

    public static final HazelcastPredicateComparison LESS_THAN = new HazelcastPredicateComparison(
        SqlStdOperatorTable.LESS_THAN
    );

    public static final HazelcastPredicateComparison LESS_THAN_OR_EQUAL = new HazelcastPredicateComparison(
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL
    );

    private HazelcastPredicateComparison(SqlBinaryOperator base) {
        super(
            base.getName(),
            base.getKind(),
            base.getLeftPrec(),
            true,
            ReturnTypes.BOOLEAN_NULLABLE,
            new OperandTypeInference(),
            null
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        SqlCallBindingOverride bindingOverride = new SqlCallBindingOverride(binding);

        return checkOperandTypes(bindingOverride, throwOnFailure);
    }

    private boolean checkOperandTypes(SqlCallBindingOverride binding, boolean throwOnFailure) {
        SqlNode first = binding.operand(0);
        SqlNode second = binding.operand(1);

        HazelcastSqlValidator validator = binding.getValidator();

        RelDataType firstType = validator.deriveType(binding.getScope(), first);
        RelDataType secondType = validator.deriveType(binding.getScope(), second);


        if (isNullLiteral(first) || isNullLiteral(second)) {
            // At least one side is NULL literal, will convert to NULL on rex-to-expression phase.
            return true;
        }

        // Now we do not have NULL at any side, proceed
        return checkOperandTypes(binding, throwOnFailure, validator, firstType, secondType);
    }

    private boolean checkOperandTypes(
        SqlCallBindingOverride callBinding,
        boolean throwOnFailure,
        HazelcastSqlValidator validator,
        RelDataType firstType,
        RelDataType secondType
    ) {
        RelDataType winningType = HazelcastTypeSystem.withHigherPrecedence(firstType, secondType);

        if (winningType == firstType) {
            return checkOperandTypesWithPrecedence(
                callBinding,
                throwOnFailure,
                validator,
                firstType,
                secondType,
                1
            );
        } else {
            assert winningType == secondType;

            return checkOperandTypesWithPrecedence(
                callBinding,
                throwOnFailure,
                validator,
                secondType,
                firstType,
                0
            );
        }
    }

    private boolean checkOperandTypesWithPrecedence(
        SqlCallBindingOverride callBinding,
        boolean throwOnFailure,
        HazelcastSqlValidator validator,
        RelDataType highType,
        RelDataType lowType,
        int lowIndex
    ) {
        QueryDataType highHZType = SqlToQueryType.map(highType.getSqlTypeName());
        QueryDataType lowHZType = SqlToQueryType.map(lowType.getSqlTypeName());

        if (highHZType.getTypeFamily() == lowHZType.getTypeFamily()) {
            // Types are in the same family, do nothing.
            return true;
        }

        if (highHZType.getTypeFamily().getGroup() != highHZType.getTypeFamily().getGroup()) {
            // Types cannot be converted to each other, throw.
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            } else {
                return false;
            }
        }

        // Types are in the same group, cast lower to higher.
        // TODO: What to do with the result?
        // TODO: Should we call validated node type?
        validator.getTypeCoercion().coerceOperandType(callBinding.getScope(), callBinding.getCall(), lowIndex, highType);

        return true;
    }

    private boolean isNullLiteral(SqlNode node) {
        if (node instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) node;

            if (call.getOperator() == HazelcastSqlLiteralFunction.INSTANCE) {
                HazelcastSqlLiteral literal = call.operand(0);

                return literal.getTypeName() == SqlTypeName.NULL;
            }
        }

        return false;
    }

    private static final class OperandTypeInference implements SqlOperandTypeInference {
        @Override
        public void inferOperandTypes(SqlCallBinding binding, RelDataType returnType, RelDataType[] operandTypes) {
            boolean hasUnknownTypes = false;
            RelDataType knownType = null;

            for (int i = 0; i < binding.getOperandCount(); i++) {
                RelDataType operandType = binding.getOperandType(i);

                if (operandType.getSqlTypeName() == SqlTypeName.NULL) {
                    hasUnknownTypes = true;
                } else {
                    operandTypes[i] = operandType;

                    if (knownType == null) {
                        knownType = operandType;
                    }
                }
            }

            if (knownType == null) {
                throw new SqlCallBindingOverride(binding).newValidationSignatureError();
            }

            if (hasUnknownTypes) {
                for (int i = 0; i < binding.getOperandCount(); i++) {
                    if (isParameter(binding.operand(i))) {
                        operandTypes[i] = knownType;
                    }
                }
            }
        }
    }
}

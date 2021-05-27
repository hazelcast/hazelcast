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

package com.hazelcast.sql.impl.calcite.validate.operators;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.createType;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.precedenceOf;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.toHazelcastType;

/**
 * A type inference algorithm that infers unknown operands to the type of
 * the operand with a known type with highest precedence.
 */
public final class UnknownFromKnownOperandTypeInference implements SqlOperandTypeInference {

    public static final UnknownFromKnownOperandTypeInference BINARY_INSTANCE =
            new UnknownFromKnownOperandTypeInference(2);
    public static final UnknownFromKnownOperandTypeInference VARIADIC_INSTANCE =
            new UnknownFromKnownOperandTypeInference(Integer.MAX_VALUE);

    private final int maxOperandCount;

    private UnknownFromKnownOperandTypeInference(int maxOperandCount) {
        if (maxOperandCount < 2) {
            throw new IllegalArgumentException("Usable for at least 2 operands");
        }
        this.maxOperandCount = maxOperandCount;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public void inferOperandTypes(SqlCallBinding binding, RelDataType returnType, RelDataType[] operandTypes) {
        assert operandTypes.length <= maxOperandCount;
        assert binding.getOperandCount() == operandTypes.length;

        boolean hasParameters = binding.operands().stream().anyMatch((operand) -> operand.getKind() == SqlKind.DYNAMIC_PARAM);

        List<Integer> unknownOperandIndexes = new ArrayList<>();
        RelDataType knownType = null;
        int knownTypePrecedence = Integer.MIN_VALUE;

        for (int i = 0; i < binding.getOperandCount(); i++) {
            operandTypes[i] = binding.getOperandType(i);

            if (!operandTypes[i].equals(binding.getValidator().getUnknownType())) {
                if (hasParameters && toHazelcastType(operandTypes[i].getSqlTypeName()).getTypeFamily().isNumericInteger()) {
                    // If we are here, the operands are a parameter and a numeric expression.
                    // We widen the type of the numeric expression to BIGINT, so that an expression `1 > ?` is resolved to
                    // `(BIGINT)1 > (BIGINT)?` rather than `(TINYINT)1 > (TINYINT)?`
                    RelDataType newOperandType = createType(
                            binding.getTypeFactory(),
                            SqlTypeName.BIGINT,
                            operandTypes[i].isNullable()
                    );

                    operandTypes[i] = newOperandType;
                }

                int precedence = precedenceOf(operandTypes[i]);
                if (knownType == null || knownType.getSqlTypeName() == SqlTypeName.NULL || knownTypePrecedence > precedence) {
                    if (operandTypes[i].getSqlTypeName() == SqlTypeName.NULL) {
                        unknownOperandIndexes.add(i);
                    } else {
                        knownType = operandTypes[i];
                        knownTypePrecedence = precedence;
                    }
                }
            } else {
                unknownOperandIndexes.add(i);
            }
        }

        // If we have [UNKNOWN, UNKNOWN] or [NULL, UNKNOWN] operands, throw a signature error,
        // since we cannot deduce the return type. [NULL, NULL] is ok.
        if (knownType == null || knownType.getSqlTypeName() == SqlTypeName.NULL && hasParameters) {
            throw new HazelcastCallBinding(binding).newValidationSignatureError();
        }

        if (isBinaryOperator()
                && SqlTypeName.INTERVAL_TYPES.contains(knownType.getSqlTypeName())
                && operandTypes[unknownOperandIndexes.get(0)].getSqlTypeName() == SqlTypeName.NULL) {
            // If there is an interval on one side and NULL on the other, assume that the other side is a TIMESTAMP,
            // because this is the only viable overload for binary operators.
            knownType = createType(binding.getTypeFactory(), SqlTypeName.TIMESTAMP, true);
        }

        for (Integer index : unknownOperandIndexes) {
            operandTypes[index] = knownType;
        }
    }

    private boolean isBinaryOperator() {
        return maxOperandCount == 2;
    }
}

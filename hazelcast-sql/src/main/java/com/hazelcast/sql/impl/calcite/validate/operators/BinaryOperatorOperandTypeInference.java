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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.createType;

public final class BinaryOperatorOperandTypeInference
        extends AbstractOperandTypeInference<BinaryOperatorOperandTypeInference.IndexState> {
    public static final BinaryOperatorOperandTypeInference INSTANCE = new BinaryOperatorOperandTypeInference();

    private BinaryOperatorOperandTypeInference() {
    }

    @Override
    protected IndexState createLocalState() {
        return new IndexState();
    }

    @Override
    protected void precondition(RelDataType[] operandTypes, SqlCallBinding binding) {
        assert operandTypes.length == 2;
        assert binding.getOperandCount() == 2;

        boolean hasParameters = binding.operands().stream().anyMatch((operand) -> operand.getKind() == SqlKind.DYNAMIC_PARAM);

        int knownTypeOperandIndex = -1;
        RelDataType knownType = null;

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

                if (knownType == null || knownType.getSqlTypeName() == SqlTypeName.NULL) {
                    knownType = operandTypes[i];
                    knownTypeOperandIndex = i;
                }
            }
        }

        // If we have [UNKNOWN, UNKNOWN] or [NULL, UNKNOWN] operands, throw a signature error,
        // since we cannot deduce the return type
        if (knownType == null || knownType.getSqlTypeName() == SqlTypeName.NULL && hasParameters) {
            throw new HazelcastCallBinding(binding).newValidationSignatureError();
        }

        if (SqlTypeName.INTERVAL_TYPES.contains(knownType.getSqlTypeName())
                && operandTypes[1 - knownTypeOperandIndex].getSqlTypeName() == SqlTypeName.NULL) {
            // If there is an interval on the one side and NULL on the other, assume that the other side is a TIMESTAMP,
            // because this is the only viable overload.
            operandTypes[1 - knownTypeOperandIndex] = createType(binding.getTypeFactory(), SqlTypeName.TIMESTAMP, true);
        } else {
            operandTypes[1 - knownTypeOperandIndex] = knownType;
        }
    }

    @Override
    protected void updateUnresolvedTypes(
            SqlCallBinding binding,
            RelDataType knownType,
            RelDataType[] operandTypes,
            IndexState state
    ) {
        // If there is an operand with an unresolved type, set it to the known type.
        if (state.unknownTypeOperandIndex != -1) {
            if (SqlTypeName.INTERVAL_TYPES.contains(knownType.getSqlTypeName())) {
                // If there is an interval on the one side, assume that the other side is a timestamp,
                // because this is the only viable overload.
                operandTypes[state.unknownTypeOperandIndex] = createType(binding.getTypeFactory(), SqlTypeName.TIMESTAMP, true);
            } else {
                operandTypes[state.unknownTypeOperandIndex] = knownType;
            }
        }
    }

    static class IndexState implements AbstractOperandTypeInference.State {
        int unknownTypeOperandIndex;

        @Override
        public void update(int index) {
            unknownTypeOperandIndex = index;
        }
    }
}

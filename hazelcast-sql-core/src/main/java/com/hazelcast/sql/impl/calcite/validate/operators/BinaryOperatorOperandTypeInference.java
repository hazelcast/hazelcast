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

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.createType;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.toHazelcastType;

public final class BinaryOperatorOperandTypeInference implements SqlOperandTypeInference {

    public static final BinaryOperatorOperandTypeInference INSTANCE = new BinaryOperatorOperandTypeInference();

    private BinaryOperatorOperandTypeInference() {
        // No-op.
    }

    @Override
    public void inferOperandTypes(SqlCallBinding binding, RelDataType returnType, RelDataType[] operandTypes) {
        assert operandTypes.length == 2;
        assert binding.getOperandCount() == 2;

        // Check if we have parameters. If yes, we will upcast integer literals to BIGINT as explained below
        boolean hasParameters = binding.operands().stream().anyMatch((operand) -> operand.getKind() == SqlKind.DYNAMIC_PARAM);

        int unknownTypeOperandIndex = -1;
        RelDataType knownType = null;

        for (int i = 0; i < binding.getOperandCount(); i++) {
            RelDataType operandType = binding.getOperandType(i);

            if (operandType.getSqlTypeName() == SqlTypeName.NULL) {
                // Will resolve operand type at this index later.
                unknownTypeOperandIndex = i;
            } else {
                if (hasParameters && toHazelcastType(operandType.getSqlTypeName()).getTypeFamily().isNumericInteger()) {
                    // If we are here, the operands are a parameter and a numeric expression.
                    // We upcast the type of the numeric expression to BIGINT, so that an expression `1 > ?` is resolved to
                    // `(BIGINT)1 > (BIGINT)?` rather than `(TINYINT)1 > (TINYINT)?`
                    RelDataType newOperandType = createType(
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

        // If we have [UNKNOWN, UNKNOWN] operands, throw a signature error, since we cannot deduce the return type
        if (knownType == null) {
            throw new HazelcastCallBinding(binding).newValidationSignatureError();
        }

        // If there is an operand with an unresolved type, set it to the known type.
        if (unknownTypeOperandIndex != -1) {
            if (SqlTypeName.INTERVAL_TYPES.contains(knownType.getSqlTypeName())) {
                // If there is an interval on the one side, assume that the other side is a timestamp,
                // because this is the only viable overload.
                operandTypes[unknownTypeOperandIndex] = createType(binding.getTypeFactory(), SqlTypeName.TIMESTAMP, true);
            } else {
                operandTypes[unknownTypeOperandIndex] = knownType;
            }
        }
    }
}

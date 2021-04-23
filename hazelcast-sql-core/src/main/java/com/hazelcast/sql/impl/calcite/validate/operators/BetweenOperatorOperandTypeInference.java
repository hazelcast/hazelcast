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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.createType;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.toHazelcastType;

public final class BetweenOperatorOperandTypeInference implements SqlOperandTypeInference {

    public static final BetweenOperatorOperandTypeInference INSTANCE = new BetweenOperatorOperandTypeInference();

    private BetweenOperatorOperandTypeInference() {
        // No-op.
    }

    @Override
    public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
        final RelDataType unknownType = callBinding.getValidator().getUnknownType();
        RelDataType knownType = unknownType;
        // compute first best-fit type.
        for (SqlNode operand : callBinding.operands()) {
            knownType = callBinding.getValidator().deriveType(callBinding.getScope(), operand);
            if (!knownType.equals(unknownType)) {
                break;
            }
        }

        // Check if we have parameters. If yes, we will upcast integer literals to BIGINT as explained below
        boolean hasParameters = callBinding.operands().stream().anyMatch((operand) -> operand.getKind() == SqlKind.DYNAMIC_PARAM);

        // Fill all types with first-known inferred type.
        Arrays.fill(operandTypes, knownType);

        // Also wide all integer numeric types to BIGINT.
        for (int i = 0; i < operandTypes.length; i++) {
            RelDataType operandType = operandTypes[i];
            if (hasParameters && toHazelcastType(operandType.getSqlTypeName()).getTypeFamily().isNumericInteger()) {
                // We upcast the type of the numeric expression to BIGINT,
                // so, an expression `1 > ?` is resolved to `(BIGINT)1 > (BIGINT)?` rather than `(TINYINT)1 > (TINYINT)?`
                RelDataType newOperandType = createType(
                        callBinding.getTypeFactory(),
                        SqlTypeName.BIGINT,
                        operandType.isNullable()
                );

                operandTypes[i] = newOperandType;
            }
        }
    }
}

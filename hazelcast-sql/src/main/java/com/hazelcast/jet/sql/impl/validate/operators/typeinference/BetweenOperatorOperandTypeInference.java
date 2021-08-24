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

package com.hazelcast.jet.sql.impl.validate.operators.typeinference;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

import java.util.Arrays;

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

        // Fill all types with first-known inferred type.
        Arrays.fill(operandTypes, knownType);
    }
}

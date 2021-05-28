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
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.precedenceOf;

/**
 * A type inference algorithm that infers unknown operands to the type of
 * the operand with a known type with highest precedence.
 */
public final class CoalesceOperandTypeInference implements SqlOperandTypeInference {

    public static final CoalesceOperandTypeInference INSTANCE = new CoalesceOperandTypeInference();

    private CoalesceOperandTypeInference() {
    }

    @Override
    public void inferOperandTypes(SqlCallBinding binding, RelDataType returnType, RelDataType[] operandTypes) {
        assert operandTypes.length > 0;
        assert binding.getOperandCount() == operandTypes.length;

        boolean hasParameters = BinaryOperatorOperandTypeInference.hasParameters(binding);

        List<Integer> unknownOperandIndexes = new ArrayList<>();
        RelDataType knownType = null;
        int knownTypePrecedence = Integer.MAX_VALUE;

        for (int i = 0; i < binding.getOperandCount(); i++) {
            operandTypes[i] = binding.getOperandType(i);

            if (!operandTypes[i].equals(binding.getValidator().getUnknownType())) {
                BinaryOperatorOperandTypeInference.castToBigIntIfNumeric(binding, operandTypes, hasParameters, i);

                int precedence = precedenceOf(operandTypes[i]);
                if (knownType == null
                        && knownTypePrecedence > precedence
                        && operandTypes[i].getSqlTypeName() != SqlTypeName.NULL) {
                    knownType = operandTypes[i];
                    knownTypePrecedence = precedence;
                }
            } else {
                unknownOperandIndexes.add(i);
            }
        }

        BinaryOperatorOperandTypeInference.throwErrorIfTypeCanNotBeDeduced(binding, knownType, hasParameters);

        for (Integer index : unknownOperandIndexes) {
            operandTypes[index] = knownType;
        }
    }
}

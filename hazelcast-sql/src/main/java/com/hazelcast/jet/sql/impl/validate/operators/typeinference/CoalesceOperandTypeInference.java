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

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.createType;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isNullOrUnknown;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.precedenceOf;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.toHazelcastType;

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
        if (operandTypes.length == 0) {
            // will be handled later
            return;
        }
        assert binding.getOperandCount() == operandTypes.length;

        boolean hasParameters = HazelcastTypeUtils.hasParameters(binding);

        List<Integer> unknownOperandIndexes = new ArrayList<>();
        RelDataType knownType = null;
        int knownTypePrecedence = Integer.MIN_VALUE;

        for (int i = 0; i < binding.getOperandCount(); i++) {
            operandTypes[i] = binding.getOperandType(i);

            if (!operandTypes[i].equals(binding.getValidator().getUnknownType())) {
                if (hasParameters && toHazelcastType(operandTypes[i]).getTypeFamily().isNumericInteger()) {
                    // If there are parameters in the operands, widen all the integers to BIGINT so that an expression
                    // `COALESCE(1, ?)` is resolved to `COALESCE((BIGINT)1, (BIGINT)?)` rather than
                    // `COALESCE((TINYINT)1, (TINYINT)?)`
                    operandTypes[i] = createType(binding.getTypeFactory(), SqlTypeName.BIGINT, operandTypes[i].isNullable());
                }

                int precedence = precedenceOf(operandTypes[i]);
                if (knownTypePrecedence < precedence) {
                    knownType = operandTypes[i];
                    knownTypePrecedence = precedence;
                }
            } else {
                unknownOperandIndexes.add(i);
            }
        }

        // If we have only UNKNOWN operands or a combination of NULL and UNKNOWN, throw a signature error,
        // since we cannot deduce the return type
        if (knownType == null || isNullOrUnknown(knownType.getSqlTypeName()) && hasParameters) {
            throw new HazelcastCallBinding(binding).newValidationSignatureError();
        }

        for (Integer index : unknownOperandIndexes) {
            operandTypes[index] = knownType;
        }
    }
}

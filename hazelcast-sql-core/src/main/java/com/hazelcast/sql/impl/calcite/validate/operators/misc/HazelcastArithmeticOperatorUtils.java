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

package com.hazelcast.sql.impl.calcite.validate.operators.misc;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.calcite.validate.operand.OperandCheckerProgram;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

public final class HazelcastArithmeticOperatorUtils {
    private HazelcastArithmeticOperatorUtils() {
        // No-op.
    }

    public static boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure, SqlKind kind) {
        RelDataType firstType = binding.getOperandType(0);
        RelDataType secondType = binding.getOperandType(1);

        if (!HazelcastTypeUtils.isNumericType(firstType) || !HazelcastTypeUtils.isNumericType(secondType)) {
            return fail(binding, throwOnFailure);
        }

        RelDataType type = HazelcastTypeUtils.withHigherPrecedence(firstType, secondType);

        switch (kind) {
            case PLUS:
            case MINUS:
            case DIVIDE:
                if (HazelcastTypeUtils.isNumericIntegerType(type)) {
                    int bitWidth = ((HazelcastIntegerType) type).getBitWidth() + 1;

                    type = HazelcastIntegerType.create(bitWidth, type.isNullable());
                }

                break;

            case TIMES:
                if (HazelcastTypeUtils.isNumericIntegerType(type)) {
                    assert firstType instanceof HazelcastIntegerType;
                    assert secondType instanceof HazelcastIntegerType;

                    int firstBitWidth = ((HazelcastIntegerType) firstType).getBitWidth();
                    int secondBitWidth = ((HazelcastIntegerType) secondType).getBitWidth();

                    type = HazelcastIntegerType.create(firstBitWidth + secondBitWidth, type.isNullable());
                }

                break;

            default:
                // For the MOD operation, we just pick the operand with a higher precedence, but do not extend the width.
                assert kind == SqlKind.MOD;

                // Like many major databases, we do not support inexact numeric operands.
                if (HazelcastTypeUtils.isNumericInexactType(type)) {
                    return fail(binding, throwOnFailure);
                }
        }

        TypedOperandChecker checker = TypedOperandChecker.forType(type);

        return new OperandCheckerProgram(
            checker,
            checker
        ).check(binding, throwOnFailure);
    }

    private static boolean fail(HazelcastCallBinding binding, boolean throwOnFailure) {
        if (throwOnFailure) {
            throw binding.newValidationSignatureError();
        } else {
            return false;
        }
    }
}

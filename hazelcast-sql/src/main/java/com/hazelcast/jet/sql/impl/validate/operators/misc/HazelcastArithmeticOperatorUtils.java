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

package com.hazelcast.jet.sql.impl.validate.operators.misc;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.OperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastIntegerType;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isIntervalType;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isNumericType;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isTemporalType;

public final class HazelcastArithmeticOperatorUtils {
    private HazelcastArithmeticOperatorUtils() {
        // No-op.
    }

    public static boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure, SqlKind kind) {
        RelDataType firstType = binding.getOperandType(0);
        RelDataType secondType = binding.getOperandType(1);

        if (isTemporalType(firstType) || isTemporalType(secondType)) {
            return checkTemporalOperands(binding, throwOnFailure, kind, firstType, secondType);
        }

        if (!isNumericType(firstType) || !isNumericType(secondType)) {
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

    /**
     * Check numeric operation with temporal operands. We allow only for {@code TEMPORAL+INTERVAL}, {@code TEMPORAL-INTERVAL}
     * and {@code INTERVAL+TEMPORAL} overloads. DATE operands are coerced to TIMESTAMP.
     */
    private static boolean checkTemporalOperands(
            HazelcastCallBinding binding,
            boolean throwOnFailure,
            SqlKind kind,
            RelDataType firstType,
            RelDataType secondType
    ) {
        if (isTemporalType(firstType)) {
            // TEMPORAL + INTERVAL or TEMPORAL - INTERVAL.
            if (isIntervalType(secondType) && (kind == SqlKind.PLUS || kind == SqlKind.MINUS)) {
                return new OperandCheckerProgram(
                        TypedOperandChecker.forType(convertToTimestampIfNeeded(binding, firstType)),
                        TypedOperandChecker.forType(secondType)
                ).check(binding, throwOnFailure);
            }
        } else {
            // INTERVAL + TEMPORAL
            assert isTemporalType(secondType);

            if (isIntervalType(firstType) && kind == SqlKind.PLUS) {
                return new OperandCheckerProgram(
                        TypedOperandChecker.forType(firstType),
                        TypedOperandChecker.forType(convertToTimestampIfNeeded(binding, secondType))
                ).check(binding, throwOnFailure);
            }
        }

        return fail(binding, throwOnFailure);
    }

    /**
     * Convert the given temporal type to timestamp.
     */
    private static RelDataType convertToTimestampIfNeeded(HazelcastCallBinding binding, RelDataType type) {
        assert isTemporalType(type);

        if (type.getSqlTypeName() == SqlTypeName.DATE) {
            return HazelcastTypeUtils.createType(binding.getTypeFactory(), SqlTypeName.TIMESTAMP, type.isNullable());
        } else {
            return type;
        }
    }

    private static boolean fail(HazelcastCallBinding binding, boolean throwOnFailure) {
        if (throwOnFailure) {
            throw binding.newValidationSignatureError();
        } else {
            return false;
        }
    }
}

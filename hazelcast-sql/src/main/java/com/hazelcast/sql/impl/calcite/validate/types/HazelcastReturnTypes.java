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

package com.hazelcast.sql.impl.calcite.validate.types;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.function.BiFunction;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType.bitWidthOf;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType.noOverflowBitWidthOf;
import static org.apache.calcite.sql.type.ReturnTypes.ARG0_INTERVAL_NULLABLE;
import static org.apache.calcite.sql.type.ReturnTypes.DECIMAL_PRODUCT_NULLABLE;
import static org.apache.calcite.sql.type.ReturnTypes.DECIMAL_QUOTIENT_NULLABLE;
import static org.apache.calcite.sql.type.ReturnTypes.DECIMAL_SUM_NULLABLE;
import static org.apache.calcite.sql.type.ReturnTypes.LEAST_RESTRICTIVE;
import static org.apache.calcite.sql.type.ReturnTypes.chain;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;

public final class HazelcastReturnTypes {

    public static final SqlReturnTypeInference NULL_IF_NULL_OPERANDS = binding -> {
        for (RelDataType type : binding.collectOperandTypes()) {
            if (type.getSqlTypeName() == NULL) {
                return type;
            }
        }
        return null;
    };

    public static final SqlReturnTypeInference PLUS = chain(NULL_IF_NULL_OPERANDS, DECIMAL_SUM_NULLABLE,
            binding -> integer(binding, HazelcastReturnTypes::binaryIntegerPlus), LEAST_RESTRICTIVE);

    public static final SqlReturnTypeInference MINUS = chain(NULL_IF_NULL_OPERANDS, DECIMAL_SUM_NULLABLE,
            binding -> integer(binding, HazelcastReturnTypes::binaryIntegerMinus), LEAST_RESTRICTIVE);

    public static final SqlReturnTypeInference MULTIPLY = chain(NULL_IF_NULL_OPERANDS, DECIMAL_PRODUCT_NULLABLE,
            binding -> integer(binding, HazelcastReturnTypes::integerMultiply), LEAST_RESTRICTIVE);

    public static final SqlReturnTypeInference DIVIDE =
            chain(NULL_IF_NULL_OPERANDS, DECIMAL_QUOTIENT_NULLABLE, ARG0_INTERVAL_NULLABLE,
                    binding -> integer(binding, HazelcastReturnTypes::integerDivide), LEAST_RESTRICTIVE);

    public static final SqlReturnTypeInference UNARY_MINUS = binding -> {
        RelDataType type = binding.getOperandType(0);
        SqlTypeName typeName = type.getSqlTypeName();
        return HazelcastIntegerType.supports(typeName) ? integerUnaryMinus(type) : type;
    };

    private HazelcastReturnTypes() {
    }

    public static RelDataType binaryIntegerPlus(RelDataType left, RelDataType right) {
        int leftBitWidth = noOverflowBitWidthOf(left);
        int rightBitWidth = noOverflowBitWidthOf(right);

        int bitWidth = Math.max(leftBitWidth, rightBitWidth);
        if (leftBitWidth != 0 && rightBitWidth != 0) {
            ++bitWidth;
        }

        return HazelcastIntegerType.of(bitWidth, left.isNullable() || right.isNullable());
    }

    public static RelDataType binaryIntegerMinus(RelDataType left, RelDataType right) {
        int leftBitWidth = noOverflowBitWidthOf(left);
        int rightBitWidth = noOverflowBitWidthOf(right);

        int bitWidth = Math.max(leftBitWidth, rightBitWidth);

        if (rightBitWidth != 0) {
            // One more bit is needed because subtraction negates the right
            // operand and that may result in overflow, e.g. 0 - Integer.MIN_VALUE.
            ++bitWidth;
        }

        if (leftBitWidth != 0 && rightBitWidth != 0) {
            ++bitWidth;
        }

        return HazelcastIntegerType.of(bitWidth, left.isNullable() || right.isNullable());
    }

    public static RelDataType integerMultiply(RelDataType left, RelDataType right) {
        int leftBitWidth = noOverflowBitWidthOf(left);
        int rightBitWidth = noOverflowBitWidthOf(right);

        int bitWidth;
        if (leftBitWidth == 0 || rightBitWidth == 0) {
            bitWidth = 0;
        } else {
            bitWidth = leftBitWidth + rightBitWidth;
        }

        return HazelcastIntegerType.of(bitWidth, left.isNullable() || right.isNullable());
    }

    public static RelDataType integerDivide(RelDataType left, RelDataType right) {
        int leftBitWidth = noOverflowBitWidthOf(left);
        // One more bit is needed because division might negate operands and
        // that may cause an overflow.
        int bitWidth = leftBitWidth == 0 ? 0 : leftBitWidth + 1;

        return HazelcastIntegerType.of(bitWidth, left.isNullable() || right.isNullable());
    }

    public static RelDataType integerUnaryMinus(RelDataType type) {
        int operandBitWidth = noOverflowBitWidthOf(type);
        int typeBitWidth = bitWidthOf(type.getSqlTypeName());

        if (operandBitWidth == typeBitWidth) {
            operandBitWidth += 1;
        }

        return HazelcastIntegerType.of(operandBitWidth, type.isNullable());
    }

    private static RelDataType integer(SqlOperatorBinding binding, BiFunction<RelDataType, RelDataType, RelDataType> infer) {
        RelDataType left = binding.getOperandType(0);
        RelDataType right = binding.getOperandType(1);

        if (!HazelcastIntegerType.supports(left.getSqlTypeName()) || !HazelcastIntegerType.supports(right.getSqlTypeName())) {
            return null;
        }

        return infer.apply(left, right);
    }

}

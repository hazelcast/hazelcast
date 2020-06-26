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

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.function.BiFunction;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType.bitWidthOf;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType.noOverflowBitWidthOf;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.typeName;
import static org.apache.calcite.sql.type.ReturnTypes.ARG0_INTERVAL_NULLABLE;
import static org.apache.calcite.sql.type.ReturnTypes.DECIMAL_PRODUCT_NULLABLE;
import static org.apache.calcite.sql.type.ReturnTypes.DECIMAL_QUOTIENT_NULLABLE;
import static org.apache.calcite.sql.type.ReturnTypes.DECIMAL_SUM_NULLABLE;
import static org.apache.calcite.sql.type.ReturnTypes.LEAST_RESTRICTIVE;
import static org.apache.calcite.sql.type.ReturnTypes.chain;

/**
 * A collection of return type inference strategies. Basically, a mirror of {@link
 * ReturnTypes} provided by Calcite with various enhancements and more precise
 * type inference.
 */
public final class HazelcastReturnTypes {

    /**
     * The same as Calcite's {@link ReturnTypes#NULLABLE_SUM}, but provides bit
     * width tracking of integer types for {@link HazelcastSqlOperatorTable#PLUS}
     * operator.
     *
     * @see HazelcastIntegerType
     */
    public static final SqlReturnTypeInference PLUS =
            chain(DECIMAL_SUM_NULLABLE, binding -> integer(binding, HazelcastReturnTypes::binaryIntegerPlus), LEAST_RESTRICTIVE);

    /**
     * The same as Calcite's {@link ReturnTypes#NULLABLE_SUM}, but provides bit
     * width tracking of integer types for {@link HazelcastSqlOperatorTable#MINUS}
     * operator.
     *
     * @see HazelcastIntegerType
     */
    public static final SqlReturnTypeInference MINUS =
            chain(DECIMAL_SUM_NULLABLE, binding -> integer(binding, HazelcastReturnTypes::binaryIntegerMinus), LEAST_RESTRICTIVE);

    /**
     * The same as Calcite's {@link ReturnTypes#PRODUCT_NULLABLE}, but provides bit
     * width tracking of integer types for {@link HazelcastSqlOperatorTable#MULTIPLY}
     * operator.
     *
     * @see HazelcastIntegerType
     */
    public static final SqlReturnTypeInference MULTIPLY =
            chain(DECIMAL_PRODUCT_NULLABLE, binding -> integer(binding, HazelcastReturnTypes::integerMultiply),
                    LEAST_RESTRICTIVE);

    /**
     * The same as Calcite's {@link ReturnTypes#QUOTIENT_NULLABLE}, but provides bit
     * width tracking of integer types for {@link HazelcastSqlOperatorTable#DIVIDE}
     * operator.
     *
     * @see HazelcastIntegerType
     */
    public static final SqlReturnTypeInference DIVIDE = chain(DECIMAL_QUOTIENT_NULLABLE, ARG0_INTERVAL_NULLABLE,
            binding -> integer(binding, HazelcastReturnTypes::integerDivide), LEAST_RESTRICTIVE);

    /**
     * Infers return type of {@link HazelcastSqlOperatorTable#UNARY_MINUS}
     * operator.
     */
    public static final SqlReturnTypeInference UNARY_MINUS = binding -> {
        RelDataType type = binding.getOperandType(0);
        return HazelcastIntegerType.supports(typeName(type)) ? integerUnaryMinus(type) : type;
    };

    private HazelcastReturnTypes() {
    }

    /**
     * Infers return type of {@link HazelcastSqlOperatorTable#PLUS} operator for
     * integers.
     */
    public static RelDataType binaryIntegerPlus(RelDataType left, RelDataType right) {
        int leftBitWidth = noOverflowBitWidthOf(left);
        int rightBitWidth = noOverflowBitWidthOf(right);

        int bitWidth = Math.max(leftBitWidth, rightBitWidth);
        if (leftBitWidth != 0 && rightBitWidth != 0) {
            ++bitWidth;
        }

        return HazelcastIntegerType.of(bitWidth, left.isNullable() || right.isNullable());
    }

    /**
     * Infers return type of {@link HazelcastSqlOperatorTable#MINUS} operator for
     * integers.
     */
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

    /**
     * Infers return type of {@link HazelcastSqlOperatorTable#MULTIPLY} operator
     * for integers.
     */
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

    /**
     * Infers return type of {@link HazelcastSqlOperatorTable#DIVIDE} operator
     * for integers.
     */
    public static RelDataType integerDivide(RelDataType left, RelDataType right) {
        int leftBitWidth = noOverflowBitWidthOf(left);
        // One more bit is needed because division might negate operands and
        // that may cause an overflow.
        int bitWidth = leftBitWidth == 0 ? 0 : leftBitWidth + 1;

        return HazelcastIntegerType.of(bitWidth, left.isNullable() || right.isNullable());
    }

    /**
     * Infers return type of {@link HazelcastSqlOperatorTable#UNARY_MINUS} operator
     * for integers.
     */
    public static RelDataType integerUnaryMinus(RelDataType type) {
        int operandBitWidth = noOverflowBitWidthOf(type);
        int typeBitWidth = bitWidthOf(typeName(type));

        if (operandBitWidth == typeBitWidth) {
            operandBitWidth += 1;
        }

        return HazelcastIntegerType.of(operandBitWidth, type.isNullable());
    }

    /**
     * Applies the given integer return type inference strategy if both operands
     * of the given binary binding are integers.
     */
    private static RelDataType integer(SqlOperatorBinding binding, BiFunction<RelDataType, RelDataType, RelDataType> strategy) {
        RelDataType left = binding.getOperandType(0);
        RelDataType right = binding.getOperandType(1);

        if (!HazelcastIntegerType.supports(typeName(left)) || !HazelcastIntegerType.supports(typeName(right))) {
            return null;
        }

        return strategy.apply(left, right);
    }

}

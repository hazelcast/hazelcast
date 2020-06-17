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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.math.MathContext;
import java.math.RoundingMode;

import static com.hazelcast.sql.impl.type.QueryDataType.MAX_DECIMAL_PRECISION;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.NULL;

/**
 * Utility methods for math functions.
 */
public final class ExpressionMath {

    /**
     * Math context used by expressions while doing math on BigDecimal values.
     */
    public static final MathContext DECIMAL_MATH_CONTEXT = new MathContext(MAX_DECIMAL_PRECISION, RoundingMode.HALF_DOWN);

    private ExpressionMath() {
        // No-op.
    }

    /**
     * Divides the left-hand side operand by the right-hand side operand.
     * <p>
     * Unlike the regular Java division operator, throws an exception if division
     * resulted in overflow.
     *
     * @param left  the left-hand side operand.
     * @param right the right-hand side operand.
     * @return a division result.
     * @throws QueryException if overflow or division by zero is detected.
     */
    public static long divideExact(long left, long right) {
        if (left == Long.MIN_VALUE && right == -1) {
            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow");
        }
        try {
            return left / right;
        } catch (ArithmeticException e) {
            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, "division by zero");
        }
    }

    /**
     * Divides the left-hand side operand by the right-hand side operand.
     * <p>
     * Unlike the regular Java division operator, throws an exception if division
     * by zero is detected.
     *
     * @param left  the left-hand side operand.
     * @param right the right-hand side operand.
     * @return a division result.
     * @throws QueryException if division by zero is detected.
     */
    public static double divideExact(double left, double right) {
        double result = left / right;
        if (Double.isInfinite(result)) {
            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, "division by zero");
        }
        return result;
    }

    /**
     * Divides the left-hand side operand by the right-hand side operand.
     * <p>
     * Unlike the regular Java division operator, throws an exception if division
     * by zero is detected.
     *
     * @param left  the left-hand side operand.
     * @param right the right-hand side operand.
     * @return a division result.
     * @throws QueryException if division by zero is detected.
     */
    public static float divideExact(float left, float right) {
        float result = left / right;
        if (Float.isInfinite(result)) {
            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, "division by zero");
        }
        return result;
    }

    public static QueryDataType inferRemainderResultType(QueryDataType type1, QueryDataType type2) {
        if (type1.getTypeFamily() == NULL || type2.getTypeFamily() == NULL) {
            return QueryDataType.NULL;
        }

        // Handle numeric types.
        if (type1 == QueryDataType.VARCHAR) {
            type1 = QueryDataType.DECIMAL;
        }

        if (type2 == QueryDataType.VARCHAR) {
            type2 = QueryDataType.DECIMAL;
        }

        if (!canConvertToNumber(type1)) {
            throw QueryException.error("Operand 1 is not numeric.");
        }

        if (!canConvertToNumber(type2)) {
            throw QueryException.error("Operand 2 is not numeric.");
        }

        return type1;
    }

    public static boolean canConvertToNumber(QueryDataType type) {
        return type.getConverter().canConvertToDecimal();
    }

}

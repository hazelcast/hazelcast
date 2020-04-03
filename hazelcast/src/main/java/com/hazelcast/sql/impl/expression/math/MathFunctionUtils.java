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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTERVAL_DAY_SECOND;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTERVAL_YEAR_MONTH;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.NULL;

/**
 * Utility methods for math functions.
 */
public final class MathFunctionUtils {

    private MathFunctionUtils() {
        // No-op.
    }

    /**
     * Infer result type.
     *
     * @param type1 Type of the first operand.
     * @param type2 Type of the second operand.
     * @return Result type.
     */
    public static QueryDataType inferPlusMinusResultType(QueryDataType type1, QueryDataType type2, boolean commutative) {
        if (type1.getTypeFamily() == NULL || type2.getTypeFamily() == NULL) {
            return QueryDataType.NULL;
        }

        // Convert strings to decimals.
        if (type1 == QueryDataType.VARCHAR) {
            type1 = QueryDataType.DECIMAL;
        }

        if (type2 == QueryDataType.VARCHAR) {
            type2 = QueryDataType.DECIMAL;
        }

        // Pick the bigger type to simplify further logic.
        if (commutative) {
            QueryDataType biggerType = QueryDataTypeUtils.withHigherPrecedence(type1, type2);

            if (biggerType == type2) {
                type2 = type1;
                type1 = biggerType;
            }
        }

        // Inference for temporal types. It should be the second one becuase interval type has higher precedence.
        if (type2.getTypeFamily().isTemporal()) {
            return inferPlusMinusResultTypeTemporal(type2, type1);
        }

        // Inference for numeric types.
        return inferPlusMinusResultTypeNumeric(type1, type2);
    }

    private static QueryDataType inferPlusMinusResultTypeNumeric(QueryDataType type1, QueryDataType type2) {
        if (!canConvertToNumber(type1)) {
            throw HazelcastSqlException.error("Operand 1 is not numeric.");
        }

        if (!canConvertToNumber(type2)) {
            throw HazelcastSqlException.error("Operand 2 is not numeric.");
        }

        QueryDataType biggerType = QueryDataTypeUtils.withHigherPrecedence(type1, type2);

        return expandPrecision(biggerType);
    }

    private static QueryDataType inferPlusMinusResultTypeTemporal(QueryDataType temporalType, QueryDataType intervalType) {
        switch (intervalType.getTypeFamily()) {
            case INTERVAL_DAY_SECOND:
            case INTERVAL_YEAR_MONTH:
                return temporalType;

            default:
                break;
        }

        throw HazelcastSqlException.error("Data type is not interval: " + intervalType);
    }

    /**
     * Infer result type for multiplication operation.
     *
     * @param type1 Type 1.
     * @param type2 Type 2.
     * @return Result type.
     */
    public static QueryDataType inferMultiplyResultType(QueryDataType type1, QueryDataType type2) {
        if (type1.getTypeFamily() == NULL || type2.getTypeFamily() == NULL) {
            return QueryDataType.NULL;
        }

        // Convert strings to decimals.
        if (type1 == QueryDataType.VARCHAR) {
            type1 = QueryDataType.DECIMAL;
        }

        if (type2 == QueryDataType.VARCHAR) {
            type2 = QueryDataType.DECIMAL;
        }

        // Pick the bigger type to simplify further logic.
        QueryDataType biggerType = QueryDataTypeUtils.withHigherPrecedence(type1, type2);

        if (biggerType == type2) {
            type2 = type1;
            type1 = biggerType;
        }

        // Handle intervals.
        if (type1.getTypeFamily() == INTERVAL_DAY_SECOND || type1.getTypeFamily() == INTERVAL_YEAR_MONTH) {
            if (!canConvertToNumber(type2)) {
                throw HazelcastSqlException.error("Operand 2 is not numeric.");
            }

            return type1;
        }

        // Handle numeric types.
        return inferMultiplyResultTypeNumeric(type1, type2);
    }

    private static QueryDataType inferMultiplyResultTypeNumeric(QueryDataType type1, QueryDataType type2) {
        // Only numeric types are allowed at this point.
        if (!canConvertToNumber(type1)) {
            throw HazelcastSqlException.error("Operand 1 is not numeric.");
        }

        if (!canConvertToNumber(type2)) {
            throw HazelcastSqlException.error("Operand 2 is not numeric.");
        }

        return expandPrecision(type1);
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    public static QueryDataType inferDivideResultType(QueryDataType type1, QueryDataType type2) {
        if (type1.getTypeFamily() == NULL || type2.getTypeFamily() == NULL) {
            return QueryDataType.NULL;
        }

        // Handle interval types.
        if (type1.getTypeFamily() == INTERVAL_YEAR_MONTH || type1.getTypeFamily() == INTERVAL_DAY_SECOND) {
            if (!canConvertToNumber(type2)) {
                throw HazelcastSqlException.error("Operand 2 is not numeric.");
            }

            return type1;
        }

        // Handle numeric types.
        if (type1 == QueryDataType.VARCHAR) {
            type1 = QueryDataType.DECIMAL;
        }

        if (type2 == QueryDataType.VARCHAR) {
            type2 = QueryDataType.DECIMAL;
        }

        if (!canConvertToNumber(type1)) {
            throw HazelcastSqlException.error("Operand 1 is not numeric.");
        }

        if (!canConvertToNumber(type2)) {
            throw HazelcastSqlException.error("Operand 2 is not numeric.");
        }

        if (type1 == QueryDataType.BIT) {
            type1 = QueryDataType.TINYINT;
        }

        return type1;
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
            throw HazelcastSqlException.error("Operand 1 is not numeric.");
        }

        if (!canConvertToNumber(type2)) {
            throw HazelcastSqlException.error("Operand 2 is not numeric.");
        }

        if (type1 == QueryDataType.BIT) {
            type1 = QueryDataType.TINYINT;
        }

        return type1;
    }

    // TODO: Optionally do not expand precision for better performance (e.g. to avoid BigDecimal's when summing longs).
    public static QueryDataType expandPrecision(QueryDataType type) {
        QueryDataTypeFamily typeFamily = type.getConverter().getTypeFamily();
        int precision = type.getPrecision();

        switch (typeFamily) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DECIMAL:
                return QueryDataTypeUtils.integerType(precision == QueryDataType.PRECISION_UNLIMITED ? precision : precision + 1);

            case REAL:
            case DOUBLE:
                return QueryDataType.DOUBLE;

            case NULL:
                return QueryDataType.NULL;

            default:
                throw new IllegalArgumentException("Type is not numeric: " + type);
        }
    }

    public static boolean canConvertToNumber(QueryDataType type) {
        return type.getConverter().canConvertToDecimal();
    }

}

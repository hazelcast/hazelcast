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
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.DataTypeUtils;
import com.hazelcast.sql.impl.type.GenericType;

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
    public static DataType inferPlusMinusResultType(DataType type1, DataType type2, boolean commutative) {
        // Handle late binding.
        if (type1.getType() == GenericType.LATE) {
            return type2;
        } else if (type2.getType() == GenericType.LATE) {
            return type1;
        }

        // Convert strings to decimals.
        if (type1 == DataType.VARCHAR) {
            type1 = DataType.DECIMAL;
        }

        if (type2 == DataType.VARCHAR) {
            type2 = DataType.DECIMAL;
        }

        // Pick the bigger type to simplify further logic.
        if (commutative) {
            DataType biggerType = DataTypeUtils.compare(type1, type2);

            if (biggerType == type2) {
                type2 = type1;
                type1 = biggerType;
            }
        }

        // Inference for temporal types. It should be the second one becuase interval type has higher precedence.
        if (type2.isTemporal()) {
            return inferPlusMinusResultTypeTemporal(type2, type1);
        }

        // Inference for numeric types.
        return inferPlusMinusResultTypeNumeric(type1, type2);
    }

    private static DataType inferPlusMinusResultTypeNumeric(DataType type1, DataType type2) {
        if (!type1.isNumeric()) {
            throw HazelcastSqlException.error("Operand 1 is not numeric.");
        }

        if (!type2.isNumeric()) {
            throw HazelcastSqlException.error("Operand 2 is not numeric.");
        }

        DataType biggerType = DataTypeUtils.compare(type1, type2);

        switch (biggerType.getType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                // Expand precision.
                return DataType.integerType(biggerType.getPrecision() + 1);

            case DECIMAL:
                return DataType.DECIMAL;

            case REAL:
            case DOUBLE:
                return DataType.DOUBLE;

            default:
                throw new IllegalStateException("Unsupported type: " + biggerType);
        }
    }

    private static DataType inferPlusMinusResultTypeTemporal(DataType temporalType, DataType intervalType) {
        switch (intervalType.getType()) {
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
    public static DataType inferMultiplyResultType(DataType type1, DataType type2) {
        // Handle late binding.
        if (type1.getType() == GenericType.LATE) {
            return type2;
        } else if (type2.getType() == GenericType.LATE) {
            return type1;
        }

        // Convert strings to decimals.
        if (type1 == DataType.VARCHAR) {
            type1 = DataType.DECIMAL;
        }

        if (type2 == DataType.VARCHAR) {
            type2 = DataType.DECIMAL;
        }

        // Pick the bigger type to simplify further logic.
        DataType biggerType = DataTypeUtils.compare(type1, type2);

        if (biggerType == type2) {
            type2 = type1;
            type1 = biggerType;
        }

        // Handle intervals.
        if (type1.getType() == GenericType.INTERVAL_DAY_SECOND || type1.getType() == GenericType.INTERVAL_YEAR_MONTH) {
            if (!type2.isNumeric()) {
                throw HazelcastSqlException.error("Operand 2 is not numeric.");
            }

            return type1;
        }

        // Handle numeric types.
        return inferMultiplyResultTypeNumeric(type1, type2);
    }

    private static DataType inferMultiplyResultTypeNumeric(DataType type1, DataType type2) {
        // Only numeric types are allowed at this point.
        if (!type1.isNumeric()) {
            throw HazelcastSqlException.error("Operand 1 is not numeric.");
        }

        if (!type2.isNumeric()) {
            throw HazelcastSqlException.error("Operand 2 is not numeric.");
        }

        switch (type1.getType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                // Expand precision.
                return DataType.integerType(type1.getPrecision() + 1);

            case DECIMAL:
                return DataType.DECIMAL;

            case REAL:
            case DOUBLE:
                return DataType.DOUBLE;

            default:
                throw new IllegalStateException("Unsupported type: " + type1);
        }
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    public static DataType inferDivideResultType(DataType type1, DataType type2) {
        // Handle late binding.
        if (type1.getType() == GenericType.LATE) {
            return type2;
        } else if (type2.getType() == GenericType.LATE) {
            return type1;
        }

        // Handle interval types.
        if (type1.getType() == GenericType.INTERVAL_YEAR_MONTH || type1.getType() == GenericType.INTERVAL_DAY_SECOND) {
            if (!type2.isNumeric()) {
                throw HazelcastSqlException.error("Operand 2 is not numeric.");
            }

            return type1;
        }

        // Handle numeric types.
        if (type1 == DataType.VARCHAR) {
            type1 = DataType.DECIMAL;
        }

        if (type2 == DataType.VARCHAR) {
            type2 = DataType.DECIMAL;
        }

        if (!type1.isNumeric()) {
            throw HazelcastSqlException.error("Operand 1 is not numeric.");
        }

        if (!type2.isNumeric()) {
            throw HazelcastSqlException.error("Operand 2 is not numeric.");
        }

        if (type1 == DataType.BIT) {
            type1 = DataType.TINYINT;
        }

        return type1;
    }

    public static DataType inferRemainderResultType(DataType type1, DataType type2) {
        // Handle late binding.
        if (type1.getType() == GenericType.LATE) {
            return type2;
        } else if (type2.getType() == GenericType.LATE) {
            return type1;
        }

        // Handle numeric types.
        if (type1 == DataType.VARCHAR) {
            type1 = DataType.DECIMAL;
        }

        if (type2 == DataType.VARCHAR) {
            type2 = DataType.DECIMAL;
        }

        if (!type1.isNumeric()) {
            throw HazelcastSqlException.error("Operand 1 is not numeric.");
        }

        if (!type2.isNumeric()) {
            throw HazelcastSqlException.error("Operand 2 is not numeric.");
        }

        if (type1 == DataType.BIT) {
            type1 = DataType.TINYINT;
        }

        return type1;
    }
}

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

package com.hazelcast.sql.impl.type;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.accessor.BigDecimalConverter;
import com.hazelcast.sql.impl.type.accessor.BigIntegerConverter;
import com.hazelcast.sql.impl.type.accessor.CalendarConverter;
import com.hazelcast.sql.impl.type.accessor.Converter;
import com.hazelcast.sql.impl.type.accessor.Converters;
import com.hazelcast.sql.impl.type.accessor.DateConverter;
import com.hazelcast.sql.impl.type.accessor.OffsetDateTimeConverter;

import java.util.List;

/**
 * Utility methods for SQL data types.
 */
public final class DataTypeUtils {
    private DataTypeUtils() {
        // No-op.
    }

    public static DataType compare(DataType first, DataType second) {
        int res = first.compareTo(second);

        return (res > 0) ? first : second;
    }

    public static DataType compare(List<DataType> types) {
        assert !types.isEmpty();

        DataType winner = null;

        for (DataType type : types) {
            if (winner == null || compare(type, winner) == type) {
                winner = type;
            }
        }

        return winner;
    }

    public static DataType compare(Expression<?>[] expressions) {
        assert expressions.length != 0;

        DataType winner = null;

        for (Expression<?> expression : expressions) {
            if (expression == null) {
                continue;
            }

            DataType type = expression.getType();

            if (winner == null || compare(type, winner) == type) {
                winner = type;
            }
        }

        return winner;
    }

    /**
     * Check if the second type could be converted to the first one.
     *
     * @param to First type.
     * @param from Second type.
     * @return {@code True} if the second type could be casted to the first one.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    public static boolean canConvertTo(DataType from, DataType to) {
        switch (to.getType()) {
            case BIT:
                return from.getConverter().canConvertToBit();

            case TINYINT:
                return from.getConverter().canConvertToTinyint();

            case SMALLINT:
                return from.getConverter().canConvertToSmallint();

            case INT:
                return from.getConverter().canConvertToInt();

            case BIGINT:
                return from.getConverter().canConvertToBigint();

            case DECIMAL:
                return from.getConverter().canConvertToDecimal();

            case REAL:
                return from.getConverter().canConvertToReal();

            case DOUBLE:
                return from.getConverter().canConvertToDouble();

            case VARCHAR:
                return from.getConverter().canConvertToVarchar();

            case DATE:
                return from.getConverter().canConvertToDate();

            case TIME:
                return from.getConverter().canConvertToTime();

            case TIMESTAMP:
                return from.getConverter().canConvertToTimestamp();

            case TIMESTAMP_WITH_TIMEZONE:
                return from.getConverter().canConvertToTimestampWithTimezone();

            case OBJECT:
                return from.getConverter().canConvertToObject();

            default:
                // TODO: Do we need to handle intervals? Should intervals be part of GenericType enum at all?
                throw new IllegalArgumentException("Unsupported type: " + to);
        }
    }

    public static void ensureCanConvertTo(DataType from, DataType to) {
        if (!DataTypeUtils.canConvertTo(from, to)) {
            throw HazelcastSqlException.error("Cannot convert " + from + " to " + to);
        }
    }

    /**
     * Get type of the given object.
     *
     * @param obj Object.
     * @return Object's type.
     */
    public static DataType resolveType(Object obj) {
        if (obj == null) {
            return DataType.LATE;
        }

        Class<?> clazz = obj.getClass();

        DataType type = resolveTypeOrNull(clazz);

        if (type == null) {
            throw HazelcastSqlException.error("Unsupported class: " + clazz.getName());
        }

        return type;
    }

    /**
     * Get type of the given object.
     *
     * @param clazz Object class.
     * @return Object's type.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    public static DataType resolveTypeOrNull(Class<?> clazz) {
        Converter converter = Converters.getConverter(clazz);

        GenericType type = converter.getGenericType();

        switch (type) {
            case BIT:
                return DataType.BIT;

            case TINYINT:
                return DataType.TINYINT;

            case SMALLINT:
                return DataType.SMALLINT;

            case INT:
                return DataType.INT;

            case BIGINT:
                return DataType.BIGINT;

            case DECIMAL:
                if (converter == BigDecimalConverter.INSTANCE) {
                    return DataType.DECIMAL;
                } else if (converter == BigIntegerConverter.INSTANCE) {
                    return DataType.DECIMAL_SCALE_0_BIG_INTEGER;
                }

                break;

            case REAL:
                return DataType.REAL;

            case DOUBLE:
                return DataType.DOUBLE;

            case VARCHAR:
                return DataType.VARCHAR;

            case DATE:
                return DataType.DATE;

            case TIME:
                return DataType.TIME;

            case TIMESTAMP:
                return DataType.TIMESTAMP;

            case TIMESTAMP_WITH_TIMEZONE:
                if (converter == DateConverter.INSTANCE) {
                    return DataType.TIMESTAMP_WITH_TIMEZONE_DATE;
                } else if (converter == CalendarConverter.INSTANCE) {
                    return DataType.TIMESTAMP_WITH_TIMEZONE_CALENDAR;
                } else if (converter == OffsetDateTimeConverter.INSTANCE) {
                    return DataType.TIMESTAMP_WITH_TIMEZONE_OFFSET_DATE_TIME;
                }

                break;

            case INTERVAL_YEAR_MONTH:
                return DataType.INTERVAL_YEAR_MONTH;

            case INTERVAL_DAY_SECOND:
                return DataType.INTERVAL_DAY_SECOND;

            case OBJECT:
                return DataType.OBJECT;

            default:
                break;
        }

        return null;
    }
}

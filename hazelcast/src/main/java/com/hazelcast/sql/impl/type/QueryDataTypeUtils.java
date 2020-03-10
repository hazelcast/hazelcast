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
import com.hazelcast.sql.impl.type.converter.BigDecimalConverter;
import com.hazelcast.sql.impl.type.converter.BigIntegerConverter;
import com.hazelcast.sql.impl.type.converter.CalendarConverter;
import com.hazelcast.sql.impl.type.converter.CharacterConverter;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import com.hazelcast.sql.impl.type.converter.DateConverter;
import com.hazelcast.sql.impl.type.converter.InstantConverter;
import com.hazelcast.sql.impl.type.converter.OffsetDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import com.hazelcast.sql.impl.type.converter.ZonedDateTimeConverter;

/**
 * Utility methods for SQL data types.
 */
public final class QueryDataTypeUtils {
    /** 12 (hdr) + 12 (fields) + 12 (arr) + 4 (arr len) + 16 (eight chars) */
    public static final int TYPE_LEN_VARCHAR = 12 + 4 + 8 + 12 + 4 + 16;

    /** 12 (hdr) + 28 (fields + padding) + 12 (int hdr) + 28 (int fields) + (12 arr hdr) + 4 (arr len) + 8 (eight digits). */
    public static final int TYPE_LEN_DECIMAL = 12 + 28 + 12 + 28 + 12 + 4 + 8;

    /** 12 (hdr) + 12 (fields + padding). */
    public static final int TYPE_LEN_TIME = 12 + 12;

    /** 12 (hdr) + 12 (fields + padding). */
    public static final int TYPE_LEN_DATE = 12 + 12;

    /** 12 (hdr) + 20 (fields + padding) + date + time. */
    public static final int TYPE_LEN_TIMESTAMP = 12 + 20 + TYPE_LEN_TIME + TYPE_LEN_DATE;

    /** 12 (hdr) + 20 (fields + padding) + timestamp + 12 (offset hdr) + 12 (offset fields). */
    public static final int TYPE_LEN_TIMESTAMP_WITH_OFFSET = 12 + 20 + TYPE_LEN_TIMESTAMP + 12 + 12;

    /** 12 (hdr) + 12 (fields). */
    public static final int TYPE_INTERVAL_DAY_SECOND = 12 + 12;

    /** 12 (hdr) + 4 (fields). */
    public static final int TYPE_INTERVAL_YEAR_MONTH = 12 + 4;

    /** 12 (hdr) + 36 (arbitrary content). */
    public static final int TYPE_LEN_OBJECT = 12 + 36;

    private static final QueryDataType[] INTEGER_TYPES = new QueryDataType[QueryDataType.PRECISION_BIGINT];

    static {
        for (int i = 1; i < QueryDataType.PRECISION_BIGINT; i++) {
            QueryDataType type;

            if (i == QueryDataType.PRECISION_BIT) {
                type = QueryDataType.BIT;
            } else if (i < QueryDataType.PRECISION_TINYINT) {
                type = new QueryDataType(QueryDataType.TINYINT.getConverter(), i);
            } else if (i == QueryDataType.PRECISION_TINYINT) {
                type = QueryDataType.TINYINT;
            } else if (i < QueryDataType.PRECISION_SMALLINT) {
                type = new QueryDataType(QueryDataType.SMALLINT.getConverter(), i);
            } else if (i == QueryDataType.PRECISION_SMALLINT) {
                type = QueryDataType.SMALLINT;
            } else if (i < QueryDataType.PRECISION_INT) {
                type = new QueryDataType(QueryDataType.INT.getConverter(), i);
            } else if (i == QueryDataType.PRECISION_INT) {
                type = QueryDataType.INT;
            } else {
                type = new QueryDataType(QueryDataType.INT.getConverter(), i);
            }

            INTEGER_TYPES[i] = type;
        }
    }

    private QueryDataTypeUtils() {
        // No-op.
    }

    public static QueryDataType bigger(QueryDataType first, QueryDataType second) {
        int res = Integer.compare(first.getTypeFamily().getPrecedence(), second.getTypeFamily().getPrecedence());

        if (res == 0) {
            res = Integer.compare(first.getPrecision(), second.getPrecision());
        }

        return (res > 0) ? first : second;
    }

    /**
     * Check if the second type could be converted to the first one.
     *
     * @param to First type.
     * @param from Second type.
     * @return {@code True} if the second type could be casted to the first one.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    public static boolean canConvertTo(QueryDataType from, QueryDataType to) {
        switch (to.getTypeFamily()) {
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
                return false;
        }
    }

    public static void ensureCanConvertTo(QueryDataType from, QueryDataType to) {
        if (!canConvertTo(from, to)) {
            throw HazelcastSqlException.error("Cannot convert " + from + " to " + to);
        }
    }

    /**
     * Get type of the given object.
     *
     * @param obj Object.
     * @return Object's type.
     */
    public static QueryDataType resolveType(Object obj) {
        if (obj == null) {
            return QueryDataType.LATE;
        }

        Class<?> clazz = obj.getClass();

        QueryDataType type = resolveTypeOrNull(clazz);

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
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:MethodLength"})
    public static QueryDataType resolveTypeOrNull(Class<?> clazz) {
        Converter converter = Converters.getConverter(clazz);

        QueryDataTypeFamily typeFamily = converter.getTypeFamily();

        switch (typeFamily) {
            case BIT:
                return QueryDataType.BIT;

            case TINYINT:
                return QueryDataType.TINYINT;

            case SMALLINT:
                return QueryDataType.SMALLINT;

            case INT:
                return QueryDataType.INT;

            case BIGINT:
                return QueryDataType.BIGINT;

            case DECIMAL:
                if (converter == BigDecimalConverter.INSTANCE) {
                    return QueryDataType.DECIMAL;
                } else if (converter == BigIntegerConverter.INSTANCE) {
                    return QueryDataType.DECIMAL_BIG_INTEGER;
                }

                break;

            case REAL:
                return QueryDataType.REAL;

            case DOUBLE:
                return QueryDataType.DOUBLE;

            case VARCHAR:
                if (converter == StringConverter.INSTANCE) {
                    return QueryDataType.VARCHAR;
                } else if (converter == CharacterConverter.INSTANCE) {
                    return QueryDataType.VARCHAR_CHARACTER;
                }

                break;

            case DATE:
                return QueryDataType.DATE;

            case TIME:
                return QueryDataType.TIME;

            case TIMESTAMP:
                return QueryDataType.TIMESTAMP;

            case TIMESTAMP_WITH_TIMEZONE:
                if (converter == DateConverter.INSTANCE) {
                    return QueryDataType.TIMESTAMP_WITH_TZ_DATE;
                } else if (converter == CalendarConverter.INSTANCE) {
                    return QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR;
                } else if (converter == InstantConverter.INSTANCE) {
                    return QueryDataType.TIMESTAMP_WITH_TZ_INSTANT;
                } else if (converter == OffsetDateTimeConverter.INSTANCE) {
                    return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
                } else if (converter == ZonedDateTimeConverter.INSTANCE) {
                    return QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME;
                }

                break;

            case INTERVAL_YEAR_MONTH:
                return QueryDataType.INTERVAL_YEAR_MONTH;

            case INTERVAL_DAY_SECOND:
                return QueryDataType.INTERVAL_DAY_SECOND;

            case OBJECT:
                return QueryDataType.OBJECT;

            default:
                break;
        }

        return null;
    }

    /**
     * Get integer type for the given precision.
     *
     * @param precision Precision.
     * @return Type.
     */
    public static QueryDataType integerType(int precision) {
        assert precision != 0;

        if (precision == QueryDataType.PRECISION_UNLIMITED) {
            return QueryDataType.DECIMAL;
        } else if (precision < QueryDataType.PRECISION_BIGINT) {
            return INTEGER_TYPES[precision];
        } else {
            return QueryDataType.DECIMAL;
        }
    }
}

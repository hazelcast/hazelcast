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

    /** 12 (hdr) + 36 (arbitrary content). */
    public static final int TYPE_LEN_OBJECT = 12 + 36;

    private static final QueryDataType[] INTEGER_TYPES = new QueryDataType[QueryDataType.PRECISION_BIGINT + 1];

    static {
        for (int i = 1; i <= QueryDataType.PRECISION_BIGINT; i++) {
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
            } else if (i < QueryDataType.PRECISION_BIGINT) {
                type = new QueryDataType(QueryDataType.BIGINT.getConverter(), i);
            } else {
                type = QueryDataType.BIGINT;
            }

            INTEGER_TYPES[i] = type;
        }
    }

    private QueryDataTypeUtils() {
        // No-op.
    }

    public static QueryDataType resolveType(Object obj) {
        if (obj == null) {
            return QueryDataType.LATE;
        }

        Class<?> clazz = obj.getClass();

        return resolveTypeForClass(clazz);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:MethodLength"})
    public static QueryDataType resolveTypeForClass(Class<?> clazz) {
        Converter converter = Converters.getConverter(clazz);

        QueryDataTypeFamily typeFamily = converter.getTypeFamily();

        switch (typeFamily) {
            case VARCHAR:
                if (converter == StringConverter.INSTANCE) {
                    return QueryDataType.VARCHAR;
                } else {
                    assert converter == CharacterConverter.INSTANCE;

                    return QueryDataType.VARCHAR_CHARACTER;
                }

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
                } else {
                    assert converter == BigIntegerConverter.INSTANCE;

                    return QueryDataType.DECIMAL_BIG_INTEGER;
                }

            case REAL:
                return QueryDataType.REAL;

            case DOUBLE:
                return QueryDataType.DOUBLE;

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
                } else {
                    assert converter == ZonedDateTimeConverter.INSTANCE;

                    return QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME;
                }

            case OBJECT:
                return QueryDataType.OBJECT;

            default:
                assert typeFamily == QueryDataTypeFamily.LATE;

                throw new IllegalArgumentException("Unexpected class: " + clazz);
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:MethodLength"})
    public static QueryDataType resolveTypeForTypeFamily(QueryDataTypeFamily typeFamily) {
        switch (typeFamily) {
            case VARCHAR:
                return QueryDataType.VARCHAR;

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
                return QueryDataType.DECIMAL;

            case REAL:
                return QueryDataType.REAL;

            case DOUBLE:
                return QueryDataType.DOUBLE;

            case DATE:
                return QueryDataType.DATE;

            case TIME:
                return QueryDataType.TIME;

            case TIMESTAMP:
                return QueryDataType.TIMESTAMP;

            case TIMESTAMP_WITH_TIMEZONE:
                return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;

            case OBJECT:
                return QueryDataType.OBJECT;

            default:
                assert typeFamily == QueryDataTypeFamily.LATE;

                throw new IllegalArgumentException("Unexpected type family: " + typeFamily);
        }
    }

    /**
     * Get integer type for the given precision.
     *
     * @param precision Precision.
     * @return Type.
     */
    public static QueryDataType integerType(int precision) {
        if (precision == 0) {
            throw new IllegalArgumentException("Precision cannot be zero.");
        }

        if (precision == QueryDataType.PRECISION_UNLIMITED) {
            return QueryDataType.DECIMAL;
        } else if (precision <= QueryDataType.PRECISION_BIGINT) {
            return INTEGER_TYPES[precision];
        } else {
            return QueryDataType.DECIMAL;
        }
    }

    public static QueryDataType bigger(QueryDataType first, QueryDataType second) {
        int res = Integer.compare(first.getTypeFamily().getPrecedence(), second.getTypeFamily().getPrecedence());

        if (res == 0) {
            res = Integer.compare(first.getPrecision(), second.getPrecision());
        }

        return (res >= 0) ? first : second;
    }
}

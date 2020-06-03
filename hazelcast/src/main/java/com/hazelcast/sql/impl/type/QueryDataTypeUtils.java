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

import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.DATE;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL_BIG_INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataType.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.INTERVAL_DAY_SECOND;
import static com.hazelcast.sql.impl.type.QueryDataType.INTERVAL_YEAR_MONTH;
import static com.hazelcast.sql.impl.type.QueryDataType.NULL;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataType.REAL;
import static com.hazelcast.sql.impl.type.QueryDataType.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataType.TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_DATE;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_INSTANT;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR_CHARACTER;

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

    // With a non-zero value we avoid weird zero-cost columns. Technically, it
    // still costs a single reference now, but reference cost is not taken into
    // account as of now.
    public static final int TYPE_LEN_NULL = 1;

    private QueryDataTypeUtils() {
        // No-op.
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:MethodLength"})
    public static QueryDataType resolveTypeForClass(Class<?> clazz) {
        Converter converter = Converters.getConverter(clazz);

        QueryDataTypeFamily typeFamily = converter.getTypeFamily();

        switch (typeFamily) {
            case VARCHAR:
                if (converter == StringConverter.INSTANCE) {
                    return VARCHAR;
                } else {
                    assert converter == CharacterConverter.INSTANCE;

                    return VARCHAR_CHARACTER;
                }

            case BOOLEAN:
                return BOOLEAN;

            case TINYINT:
                return TINYINT;

            case SMALLINT:
                return SMALLINT;

            case INT:
                return INT;

            case BIGINT:
                return BIGINT;

            case DECIMAL:
                if (converter == BigDecimalConverter.INSTANCE) {
                    return DECIMAL;
                } else {
                    assert converter == BigIntegerConverter.INSTANCE;

                    return DECIMAL_BIG_INTEGER;
                }

            case REAL:
                return REAL;

            case DOUBLE:
                return DOUBLE;

            case DATE:
                return DATE;

            case TIME:
                return TIME;

            case TIMESTAMP:
                return TIMESTAMP;

            case TIMESTAMP_WITH_TIME_ZONE:
                if (converter == DateConverter.INSTANCE) {
                    return TIMESTAMP_WITH_TZ_DATE;
                } else if (converter == CalendarConverter.INSTANCE) {
                    return TIMESTAMP_WITH_TZ_CALENDAR;
                } else if (converter == InstantConverter.INSTANCE) {
                    return TIMESTAMP_WITH_TZ_INSTANT;
                } else if (converter == OffsetDateTimeConverter.INSTANCE) {
                    return TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
                } else {
                    assert converter == ZonedDateTimeConverter.INSTANCE;

                    return TIMESTAMP_WITH_TZ_ZONED_DATE_TIME;
                }

            case INTERVAL_YEAR_MONTH:
                return INTERVAL_YEAR_MONTH;

            case INTERVAL_DAY_SECOND:
                return INTERVAL_DAY_SECOND;

            case OBJECT:
                return OBJECT;

            case NULL:
                return NULL;

            default:
                throw new IllegalArgumentException("Unexpected class: " + clazz);
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:MethodLength"})
    public static QueryDataType resolveTypeForTypeFamily(QueryDataTypeFamily typeFamily) {
        switch (typeFamily) {
            case VARCHAR:
                return VARCHAR;

            case BOOLEAN:
                return BOOLEAN;

            case TINYINT:
                return TINYINT;

            case SMALLINT:
                return SMALLINT;

            case INT:
                return INT;

            case BIGINT:
                return BIGINT;

            case DECIMAL:
                return DECIMAL;

            case REAL:
                return REAL;

            case DOUBLE:
                return DOUBLE;

            case DATE:
                return DATE;

            case TIME:
                return TIME;

            case TIMESTAMP:
                return TIMESTAMP;

            case TIMESTAMP_WITH_TIME_ZONE:
                return TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;

            case INTERVAL_YEAR_MONTH:
                return INTERVAL_YEAR_MONTH;

            case INTERVAL_DAY_SECOND:
                return INTERVAL_DAY_SECOND;

            case OBJECT:
                return OBJECT;

            case NULL:
                return NULL;

            default:
                throw new IllegalArgumentException("Unexpected type family: " + typeFamily);
        }
    }

}

/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import static com.hazelcast.sql.impl.type.QueryDataType.JSON;
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
 * <p>
 * Length descriptions are generated using https://github.com/openjdk/jol.
 */
public final class QueryDataTypeUtils {
    /**
     * java.lang.String footprint:
     * COUNT       AVG       SUM   DESCRIPTION
     *     1        32        32   [C
     *     1        24        24   java.lang.String
     *     2                  56   (total)
     */
    public static final int TYPE_LEN_VARCHAR = 56;

    /**
     * java.math.BigDecimal footprint:
     * COUNT       AVG       SUM   DESCRIPTION
     *     1        32        32   [I
     *     1        40        40   java.math.BigDecimal
     *     1        40        40   java.math.BigInteger
     *     3                 112   (total)
     */
    public static final int TYPE_LEN_DECIMAL = 112;

    /**
     * java.time.LocalTime footprint:
     * COUNT       AVG       SUM   DESCRIPTION
     *     1        24        24   java.time.LocalTime
     *     1                  24   (total)
     */
    public static final int TYPE_LEN_TIME = 24;

    /**
     * java.time.LocalDate footprint:
     * COUNT       AVG       SUM   DESCRIPTION
     *     1        24        24   java.time.LocalDate
     *     1                  24   (total)
     */
    public static final int TYPE_LEN_DATE = 24;

    /**
     * java.time.LocalDateTime footprint:
     * COUNT       AVG       SUM   DESCRIPTION
     *     1        24        24   java.time.LocalDate
     *     1        24        24   java.time.LocalDateTime
     *     1        24        24   java.time.LocalTime
     *     3                  72   (total)
     */
    public static final int TYPE_LEN_TIMESTAMP = 72;

    /**
     * java.time.OffsetDateTime footprint:
     * COUNT       AVG       SUM   DESCRIPTION
     *     1        24        24   [C
     *     1        24        24   java.lang.String
     *     1        24        24   java.time.LocalDate
     *     1        24        24   java.time.LocalDateTime
     *     1        24        24   java.time.LocalTime
     *     1        24        24   java.time.OffsetDateTime
     *     1        24        24   java.time.ZoneOffset
     *     7                 168   (total)
     */
    public static final int TYPE_LEN_TIMESTAMP_WITH_TIME_ZONE = 168;

    /**
     * com.hazelcast.sql.impl.type.SqlYearMonthInterval footprint:
     * COUNT       AVG       SUM   DESCRIPTION
     *     1        16        16   com.hazelcast.sql.impl.type.SqlYearMonthInterval
     *     1                  16   (total)
     */
    public static final int TYPE_LEN_INTERVAL_YEAR_MONTH = 16;

    /**
     * com.hazelcast.sql.impl.type.SqlDaySecondInterval footprint:
     * COUNT       AVG       SUM   DESCRIPTION
     *     1        24        24   com.hazelcast.sql.impl.type.SqlDaySecondInterval
     *     1                  24   (total)
     */
    public static final int TYPE_LEN_INTERVAL_DAY_SECOND = 24;

    /**
     * java.util.HashMap footprint:
     * COUNT       AVG       SUM   DESCRIPTION
     *     1        48        48   java.util.HashMap
     *     1                  48   (total)
     *
     * Empty map.
     */
    public static final int TYPE_LEN_MAP = 48;

    /**
     * TODO: replace footprint with actual value?
     *
     * True VARCHAR footprint is 40, old values are kept for now, therefore new value is computed
     * as VARCHAR footprint + 16 (reference/class footprint).
     */
    public static final int TYPE_LEN_JSON = TYPE_LEN_VARCHAR + 16;

    /** 12 (hdr) + 36 (arbitrary content). */
    public static final int TYPE_LEN_OBJECT = 12 + 36;

    // With a non-zero value we avoid weird zero-cost columns. Technically, it
    // still costs a single reference now, but reference cost is not taken into
    // account as of now.
    public static final int TYPE_LEN_NULL = 1;

    public static final int PRECEDENCE_NULL = 0;
    public static final int PRECEDENCE_VARCHAR = 100;
    public static final int PRECEDENCE_BOOLEAN = 200;
    public static final int PRECEDENCE_TINYINT = 300;
    public static final int PRECEDENCE_SMALLINT = 400;
    public static final int PRECEDENCE_INTEGER = 500;
    public static final int PRECEDENCE_BIGINT = 600;
    public static final int PRECEDENCE_DECIMAL = 700;
    public static final int PRECEDENCE_REAL = 800;
    public static final int PRECEDENCE_DOUBLE = 900;
    public static final int PRECEDENCE_TIME = 1000;
    public static final int PRECEDENCE_DATE = 1100;
    public static final int PRECEDENCE_TIMESTAMP = 1200;
    public static final int PRECEDENCE_TIMESTAMP_WITH_TIME_ZONE = 1300;
    public static final int PRECEDENCE_OBJECT = 1400;
    public static final int PRECEDENCE_INTERVAL_YEAR_MONTH = 10;
    public static final int PRECEDENCE_INTERVAL_DAY_SECOND = 20;
    public static final int PRECEDENCE_MAP = 30;
    public static final int PRECEDENCE_JSON = 40;

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

            case INTEGER:
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

            case OBJECT:
            case MAP:
                return OBJECT;

            case NULL:
                return NULL;

            case JSON:
                return JSON;

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

            case INTEGER:
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

            case OBJECT:
                return OBJECT;

            case NULL:
                return NULL;

            case JSON:
                return JSON;

            default:
                throw new IllegalArgumentException("Unexpected type family: " + typeFamily);
        }
    }

    public static boolean isNumeric(QueryDataType type) {
        return isNumeric(type.getTypeFamily());
    }

    public static boolean isNumeric(QueryDataTypeFamily typeFamily) {
        switch (typeFamily) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case DOUBLE:
                return true;

            default:
                return false;
        }
    }
}

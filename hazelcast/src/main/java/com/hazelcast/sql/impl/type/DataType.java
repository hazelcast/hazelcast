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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.type.accessor.BigDecimalConverter;
import com.hazelcast.sql.impl.type.accessor.BigIntegerConverter;
import com.hazelcast.sql.impl.type.accessor.BooleanConverter;
import com.hazelcast.sql.impl.type.accessor.ByteConverter;
import com.hazelcast.sql.impl.type.accessor.CalendarConverter;
import com.hazelcast.sql.impl.type.accessor.Converter;
import com.hazelcast.sql.impl.type.accessor.Converters;
import com.hazelcast.sql.impl.type.accessor.DateConverter;
import com.hazelcast.sql.impl.type.accessor.DoubleConverter;
import com.hazelcast.sql.impl.type.accessor.FloatConverter;
import com.hazelcast.sql.impl.type.accessor.IntegerConverter;
import com.hazelcast.sql.impl.type.accessor.LocalDateConverter;
import com.hazelcast.sql.impl.type.accessor.LocalDateTimeConverter;
import com.hazelcast.sql.impl.type.accessor.LocalTimeConverter;
import com.hazelcast.sql.impl.type.accessor.LongConverter;
import com.hazelcast.sql.impl.type.accessor.ObjectConverter;
import com.hazelcast.sql.impl.type.accessor.OffsetDateTimeConverter;
import com.hazelcast.sql.impl.type.accessor.ShortConverter;
import com.hazelcast.sql.impl.type.accessor.SqlDaySecondIntervalConverter;
import com.hazelcast.sql.impl.type.accessor.SqlYearMonthIntervalConverter;
import com.hazelcast.sql.impl.type.accessor.StringConverter;

/**
 * Data type represents a type of concrete expression which is based on some basic data type.
 */
public class DataType {
    /** Constant: unlimited precision. */
    public static final int PRECISION_UNLIMITED = -1;

    /** Precision of BOOLEAN. */
    public static final int PRECISION_BIT = 1;

    /** Precision of TINYINT. */
    public static final int PRECISION_TINYINT = 4;

    /** Precision of SMALLINT. */
    public static final int PRECISION_SMALLINT = 7;

    /** Precision of INT. */
    public static final int PRECISION_INT = 11;

    /** Precision of BIGINT */
    public static final int PRECISION_BIGINT = 20;

    /** Scale for integer numeric type. */
    public static final int SCALE_INTEGER = 0;

    /** Scale for division operation. */
    public static final int SCALE_DIVIDE = 38;

    /** Constant: unlimited scale. */
    public static final int SCALE_UNLIMITED = -1;

    public static final int PRECEDENCE_LATE = 0;
    public static final int PRECEDENCE_VARCHAR = 100;
    public static final int PRECEDENCE_BIT = 200;
    public static final int PRECEDENCE_TINYINT = 300;
    public static final int PRECEDENCE_SMALLINT = 400;
    public static final int PRECEDENCE_INT = 500;
    public static final int PRECEDENCE_BIGINT = 600;
    public static final int PRECEDENCE_DECIMAL = 700;
    public static final int PRECEDENCE_REAL = 800;
    public static final int PRECEDENCE_DOUBLE = 900;
    public static final int PRECEDENCE_INTERVAL_YEAR_MONTH = 1000;
    public static final int PRECEDENCE_INTERVAL_DAY_SECOND = 1100;
    public static final int PRECEDENCE_TIME = 1200;
    public static final int PRECEDENCE_DATE = 1300;
    public static final int PRECEDENCE_TIMESTAMP = 1400;
    public static final int PRECEDENCE_TIMESTAMP_WITH_TIMEZONE = 1500;
    public static final int PRECEDENCE_OBJECT = 1600;

    /** LATE (unresolved) data type. */
    public static final DataType LATE = new DataType(
        null,
        null,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_LATE
    );

    /** VARCHAR data type. */
    public static final DataType VARCHAR = new DataType(
        GenericType.VARCHAR,
        StringConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_VARCHAR
    );

    /** BIT data type. */
    public static final DataType BIT = new DataType(
        GenericType.BIT,
        BooleanConverter.INSTANCE,
        PRECISION_BIT,
        SCALE_INTEGER,
        PRECEDENCE_BIT
    );

    /** TINYINT data type. */
    public static final DataType TINYINT = new DataType(
        GenericType.TINYINT,
        ByteConverter.INSTANCE,
        PRECISION_TINYINT,
        SCALE_INTEGER,
        PRECEDENCE_TINYINT
    );

    /** SMALLINT data type. */
    public static final DataType SMALLINT = new DataType(
        GenericType.SMALLINT,
        ShortConverter.INSTANCE,
        PRECISION_SMALLINT,
        SCALE_INTEGER,
        PRECEDENCE_SMALLINT
    );

    /** INT data type. */
    public static final DataType INT = new DataType(
        GenericType.INT,
        IntegerConverter.INSTANCE,
        PRECISION_INT,
        SCALE_INTEGER,
        PRECEDENCE_INT
    );

    /** BIGINT data type. */
    public static final DataType BIGINT = new DataType(
        GenericType.BIGINT,
        LongConverter.INSTANCE,
        PRECISION_BIGINT,
        SCALE_INTEGER,
        PRECEDENCE_BIGINT
    );

    /** DECIMAL data type. */
    public static final DataType DECIMAL = new DataType(
        GenericType.DECIMAL,
        BigDecimalConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_DECIMAL
    );

    /** DECIMAL data type created from BigInteger instance. */
    public static final DataType DECIMAL_SCALE_0_BIG_INTEGER = new DataType(
        GenericType.DECIMAL,
        BigIntegerConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_INTEGER,
        PRECEDENCE_DECIMAL
    );

    /** DECIMAL data type with zero scale created from BigDecimal instance. */
    public static final DataType DECIMAL_SCALE_0_BIG_DECIMAL = new DataType(
        GenericType.DECIMAL,
        BigDecimalConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_INTEGER,
        PRECEDENCE_DECIMAL
    );

    /** REAL data type. */
    public static final DataType REAL = new DataType(
        GenericType.REAL,
        FloatConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_REAL
    );

    /** DOUBLE data type. */
    public static final DataType DOUBLE = new DataType(
        GenericType.DOUBLE,
        DoubleConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_DOUBLE
    );

    /** TIME data type. */
    public static final DataType TIME = new DataType(
        GenericType.TIME,
        LocalTimeConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_TIME
    );

    /** DATE data type. */
    public static final DataType DATE = new DataType(
        GenericType.DATE,
        LocalDateConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_DATE
    );

    /** TIMESTAMP data type. */
    public static final DataType TIMESTAMP = new DataType(
        GenericType.TIMESTAMP,
        LocalDateTimeConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_TIMESTAMP
    );

    /** TIMESTAMP WITH TIMEZONE data type created out of java.util.Date. */
    public static final DataType TIMESTAMP_WITH_TIMEZONE_DATE = new DataType(
        GenericType.TIMESTAMP_WITH_TIMEZONE,
        DateConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_TIMESTAMP_WITH_TIMEZONE
    );

    /** TIMESTAMP WITH TIMEZONE data type created out of java.util.Calendar. */
    public static final DataType TIMESTAMP_WITH_TIMEZONE_CALENDAR = new DataType(
        GenericType.TIMESTAMP_WITH_TIMEZONE,
        CalendarConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_TIMESTAMP_WITH_TIMEZONE
    );

    /** TIMESTAMP WITH TIMEZONE data type created out of java.time.OffsetDateTime. */
    public static final DataType TIMESTAMP_WITH_TIMEZONE_OFFSET_DATE_TIME = new DataType(
        GenericType.TIMESTAMP_WITH_TIMEZONE,
        OffsetDateTimeConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_TIMESTAMP_WITH_TIMEZONE
    );

    /** Interval year-month. */
    public static final DataType INTERVAL_YEAR_MONTH = new DataType(
        GenericType.INTERVAL_YEAR_MONTH,
        SqlYearMonthIntervalConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_INTERVAL_YEAR_MONTH
    );

    /** Interval day-second. */
    public static final DataType INTERVAL_DAY_SECOND = new DataType(
        GenericType.INTERVAL_DAY_SECOND,
        SqlDaySecondIntervalConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_INTERVAL_DAY_SECOND
    );

    /** Object data type. */
    public static final DataType OBJECT = new DataType(
        GenericType.OBJECT,
        ObjectConverter.INSTANCE,
        PRECISION_UNLIMITED,
        SCALE_UNLIMITED,
        PRECEDENCE_OBJECT
    );

    /** Common cached integer data types. */
    private static final DataType[] INTEGER_TYPES = new DataType[PRECISION_BIGINT];

    /** Underlying generic type. */
    private final GenericType type;

    /** Converter. */
    private final Converter converter;

    /** Precision. */
    private final int precision;

    /** Scale. */
    private final int scale;

    /** Precedence */
    private final int precedence;

    static {
        for (int i = 1; i < PRECISION_BIGINT; i++) {
            DataType type;

            if (i == PRECISION_BIT) {
                type = DataType.BIT;
            } else if (i < PRECISION_TINYINT) {
                type = new DataType(
                    TINYINT.type,
                    TINYINT.converter,
                    i,
                    TINYINT.scale,
                    TINYINT.precedence
                );
            } else if (i == PRECISION_TINYINT) {
                type = DataType.TINYINT;
            } else if (i < PRECISION_SMALLINT) {
                type = new DataType(
                    SMALLINT.type,
                    SMALLINT.converter,
                    i,
                    SMALLINT.scale,
                    SMALLINT.precedence
                );
            } else if (i == PRECISION_SMALLINT) {
                type = DataType.SMALLINT;
            } else if (i < PRECISION_INT) {
                type = new DataType(
                    INT.type,
                    INT.converter,
                    i,
                    INT.scale,
                    INT.precedence
                );
            } else if (i == PRECISION_INT) {
                type = DataType.INT;
            } else {
                type = new DataType(
                    INT.type,
                    INT.converter,
                    i,
                    INT.scale,
                    INT.precedence
                );
            }

            INTEGER_TYPES[i] = type;
        }
    }

    public DataType(GenericType type, Converter converter, int precision, int scale, int precedence) {
        this.type = type;
        this.converter = converter;
        this.precision = precision;
        this.scale = scale;
        this.precedence = precedence;
    }

    /**
     * Get type of the given object.
     *
     * @param obj Object.
     * @return Object's type.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    public static DataType resolveType(Object obj) {
        if (obj == null) {
            return LATE;
        }

        Converter converter = Converters.getConverter(obj);

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

        throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Unsupported class: " + obj.getClass().getName());
    }

    /**
     * Get integer type for the given precision.
     *
     * @param precision Precision.
     * @return Type.
     */
    public static DataType integerType(int precision) {
        assert precision != 0;

        if (precision == PRECISION_UNLIMITED) {
            return DataType.DECIMAL_SCALE_0_BIG_DECIMAL;
        } else if (precision < PRECISION_BIGINT) {
            return INTEGER_TYPES[precision];
        } else {
            return DECIMAL_SCALE_0_BIG_DECIMAL;
        }
    }

    /**
     * Return passed data type or {@link DataType#LATE} if the argument is {@code null}.
     *
     * @param type Type.
     * @return Same type or {@link DataType#LATE}.
     */
    public static DataType notNullOrLate(DataType type) {
        return type != null ? type : DataType.LATE;
    }

    public GenericType getType() {
        return type;
    }

    public Converter getConverter() {
        return converter;
    }

    public int getPrecedence() {
        return precedence;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public void ensureSame(Object val) {
        if (val != null) {
            DataType other = resolveType(val);

            if (converter != other.converter) {
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Type mismatch {expected=" + this
                    + ", actual=" + other + '}');
            }
        }
    }

    public boolean isCanConvertToNumeric() {
        return type.isConvertToNumeric();
    }

    public boolean isTemporal() {
        return type.isTemporal();
    }

    @Override
    public String toString() {
        return "DataType{base=" + type.name() + ", precision=" + precision + ", scale=" + scale + "}";
    }
}

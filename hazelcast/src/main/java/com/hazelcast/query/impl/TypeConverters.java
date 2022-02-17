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

package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.nio.serialization.Portable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.TimeZone;
import static com.hazelcast.query.impl.AbstractIndex.NULL;

public final class TypeConverters {

    public static final TypeConverter BIG_INTEGER_CONVERTER = new BigIntegerConverter();
    public static final TypeConverter BIG_DECIMAL_CONVERTER = new BigDecimalConverter();
    public static final TypeConverter DOUBLE_CONVERTER = new DoubleConverter();
    public static final TypeConverter LONG_CONVERTER = new LongConverter();
    public static final TypeConverter INTEGER_CONVERTER = new IntegerConverter();
    public static final TypeConverter BOOLEAN_CONVERTER = new BooleanConverter();
    public static final TypeConverter SHORT_CONVERTER = new ShortConverter();
    public static final TypeConverter FLOAT_CONVERTER = new FloatConverter();
    public static final TypeConverter STRING_CONVERTER = new StringConverter();
    public static final TypeConverter CHAR_CONVERTER = new CharConverter();
    public static final TypeConverter BYTE_CONVERTER = new ByteConverter();
    public static final TypeConverter ENUM_CONVERTER = new EnumConverter();
    public static final TypeConverter SQL_DATE_CONVERTER = new SqlDateConverter();
    public static final TypeConverter SQL_TIMESTAMP_CONVERTER = new SqlTimestampConverter();
    public static final TypeConverter DATE_CONVERTER = new DateConverter();
    public static final TypeConverter IDENTITY_CONVERTER = new IdentityConverter();
    public static final TypeConverter NULL_CONVERTER = new IdentityConverter();
    public static final TypeConverter UUID_CONVERTER = new UUIDConverter();
    public static final TypeConverter PORTABLE_CONVERTER = new PortableConverter();
    public static final TypeConverter LOCAL_TIME_CONVERTER = new SqlLocalTimeConverter();
    public static final TypeConverter LOCAL_DATE_CONVERTER = new SqlLocalDateConverter();
    public static final TypeConverter LOCAL_DATE_TIME_CONVERTER = new SqlLocalDateTimeConverter();
    public static final TypeConverter OFFSET_DATE_TIME_CONVERTER = new SqlOffsetDateTimeConverter();

    private TypeConverters() {
    }

    public abstract static class BaseTypeConverter implements TypeConverter {

        abstract Comparable convertInternal(Comparable value);

        public final Comparable convert(Comparable value) {
            if (value == null || value == NULL) {
                return NULL;
            }
            return convertInternal(value);
        }

    }

    static class EnumConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            String enumString = value.toString();
            if (enumString.contains(".")) {
                // there is a dot  in the value specifier, keep part after last dot
                enumString = enumString.substring(1 + enumString.lastIndexOf('.'));
            }
            return enumString;
        }

    }

    static class SqlDateConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof java.util.Date) {
                return value;
            }
            if (value instanceof String) {
                return DateHelper.parseSqlDate((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return new java.sql.Date(number.longValue());
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to java.sql.Date");
        }

    }

    static class SqlTimestampConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof Timestamp) {
                return value;
            }
            if (value instanceof java.util.Date) {
                return new Timestamp(((Date) value).getTime());
            }
            if (value instanceof String) {
                return DateHelper.parseTimeStamp((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return new Timestamp(number.longValue());
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to java.sql.Timestamp");
        }

    }

    static class DateConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof Date) {
                return value;
            }
            if (value instanceof String) {
                return DateHelper.parseDate((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return new Date(number.longValue());
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to java.util.Date");
        }

    }

    static class DoubleConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            Class clazz = value.getClass();

            if (clazz == Double.class) {
                return value;
            }

            if (value instanceof Number) {
                Number number = (Number) value;

                if (clazz == Long.class) {
                    double doubleValue = number.doubleValue();
                    if (Numbers.equalLongAndDouble(number.longValue(), doubleValue)) {
                        return doubleValue;
                    }
                } else if (clazz == Integer.class || clazz == Float.class || clazz == Short.class || clazz == Byte.class) {
                    return number.doubleValue();
                }

                return value;
            }

            if (value instanceof String) {
                return Double.parseDouble((String) value);
            }

            throw new IllegalArgumentException("Cannot convert [" + value + "] to number");
        }

    }

    static class LongConverter extends BaseTypeConverter {

        @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
        @Override
        Comparable convertInternal(Comparable value) {
            Class clazz = value.getClass();

            if (clazz == Long.class) {
                return value;
            }

            if (value instanceof Number) {
                Number number = (Number) value;

                if (clazz == Double.class) {
                    long longValue = number.longValue();
                    if (Numbers.equalDoubles(number.doubleValue(), (double) longValue)) {
                        return longValue;
                    }
                } else if (clazz == Float.class) {
                    long longValue = number.longValue();
                    if (Numbers.equalFloats(number.floatValue(), (float) longValue)) {
                        return longValue;
                    }
                } else if (clazz == Integer.class || clazz == Short.class || clazz == Byte.class) {
                    return number.longValue();
                }

                return value;
            }

            if (value instanceof String) {
                String string = (String) value;

                try {
                    return Long.parseLong(string);
                } catch (NumberFormatException e) {
                    double parsedDouble = Double.parseDouble(string);

                    long longValue = (long) parsedDouble;
                    if (Numbers.equalDoubles(parsedDouble, (double) longValue)) {
                        return longValue;
                    }

                    return parsedDouble;
                }
            }

            throw new IllegalArgumentException("Cannot convert [" + value + "] to number");
        }

    }

    static class BigIntegerConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof BigInteger) {
                return value;
            }
            if (value instanceof BigDecimal) {
                BigDecimal decimal = (BigDecimal) value;
                return decimal.toBigInteger();
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return BigInteger.valueOf(number.longValue());
            }
            if (value instanceof Boolean) {
                return ((Boolean) value) ? BigInteger.ONE : BigInteger.ZERO;
            }
            return new BigInteger(value.toString());
        }

    }

    static class BigDecimalConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof BigDecimal) {
                return value;
            }
            if (value instanceof BigInteger) {
                return new BigDecimal((BigInteger) value);
            }
            if (isIntegralDataType(value)) {
                Number number = (Number) value;
                return BigDecimal.valueOf(number.longValue());
            }
            if (isFloatingPointDataType(value)) {
                Number number = (Number) value;
                return BigDecimal.valueOf(number.doubleValue());
            }
            if (value instanceof Boolean) {
                return ((Boolean) value) ? BigDecimal.ONE : BigDecimal.ZERO;
            }
            return new BigDecimal(value.toString());
        }

        private boolean isFloatingPointDataType(Comparable value) {
            return value instanceof Double || value instanceof Float;
        }

        private boolean isIntegralDataType(Comparable value) {
            return value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
        }

    }

    static class IntegerConverter extends BaseTypeConverter {

        @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:returncount"})
        @Override
        Comparable convertInternal(Comparable value) {
            Class clazz = value.getClass();

            if (clazz == Integer.class) {
                return value;
            }

            if (value instanceof Number) {
                Number number = (Number) value;

                if (clazz == Long.class) {
                    int intValue = number.intValue();
                    if (number.longValue() == (long) intValue) {
                        return intValue;
                    }
                } else if (clazz == Double.class) {
                    int intValue = number.intValue();
                    if (Numbers.equalDoubles(number.doubleValue(), (double) intValue)) {
                        return intValue;
                    }
                } else if (clazz == Float.class) {
                    int intValue = number.intValue();
                    if (Numbers.equalFloats(number.floatValue(), (float) intValue)) {
                        return intValue;
                    }
                } else if (clazz == Short.class || clazz == Byte.class) {
                    return number.intValue();
                }

                return value;
            }

            if (value instanceof String) {
                String string = (String) value;

                try {
                    return Integer.parseInt(string);
                } catch (NumberFormatException e1) {
                    try {
                        return Long.parseLong(string);
                    } catch (NumberFormatException e2) {
                        double parsedDouble = Double.parseDouble(string);

                        int intValue = (int) parsedDouble;
                        if (Numbers.equalDoubles(parsedDouble, (double) intValue)) {
                            return intValue;
                        }

                        return parsedDouble;
                    }
                }
            }

            throw new IllegalArgumentException("Cannot convert [" + value + "] to number");
        }

    }

    static class StringConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof String) {
                return value;
            }
            return value.toString();
        }

    }

    static class FloatConverter extends BaseTypeConverter {

        @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
        @Override
        Comparable convertInternal(Comparable value) {
            Class clazz = value.getClass();

            if (clazz == Float.class) {
                return value;
            }

            if (value instanceof Number) {
                Number number = (Number) value;

                if (clazz == Double.class) {
                    float floatValue = number.floatValue();
                    if (number.doubleValue() == (double) floatValue) {
                        // That almost never happens for a random double, but
                        // users tend to query on whole numbers which are
                        // frequently may be represented as a float.
                        return floatValue;
                    }
                } else if (clazz == Long.class) {
                    float floatValue = number.floatValue();
                    if (Numbers.equalLongAndDouble(number.longValue(), floatValue)) {
                        return floatValue;
                    }
                } else if (clazz == Integer.class) {
                    float floatValue = number.floatValue();
                    if (Numbers.equalLongAndDouble(number.intValue(), floatValue)) {
                        return floatValue;
                    }
                } else if (clazz == Short.class || clazz == Byte.class) {
                    return number.floatValue();
                }

                return value;
            }

            if (value instanceof String) {
                // Using parseDouble instead of parseFloat to guarantee the most
                // precise representation.
                double parsedDouble = Double.parseDouble((String) value);

                float floatValue = (float) parsedDouble;
                if (parsedDouble == (double) floatValue) {
                    return floatValue;
                }

                return parsedDouble;
            }

            throw new IllegalArgumentException("Cannot convert [" + value + "] to number");
        }

    }

    static class ShortConverter extends BaseTypeConverter {

        @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:returncount"})
        @Override
        Comparable convertInternal(Comparable value) {
            Class clazz = value.getClass();

            if (clazz == Short.class) {
                return value;
            }

            if (value instanceof Number) {
                Number number = (Number) value;

                if (clazz == Long.class) {
                    short shortValue = number.shortValue();
                    if (number.longValue() == (long) shortValue) {
                        return shortValue;
                    }
                } else if (clazz == Double.class) {
                    short shortValue = number.shortValue();
                    if (Numbers.equalDoubles(number.doubleValue(), (double) shortValue)) {
                        return shortValue;
                    }
                } else if (clazz == Integer.class) {
                    short shortValue = number.shortValue();
                    if (number.intValue() == (int) shortValue) {
                        return shortValue;
                    }
                } else if (clazz == Float.class) {
                    short shortValue = number.shortValue();
                    if (Numbers.equalFloats(number.floatValue(), (float) shortValue)) {
                        return shortValue;
                    }
                } else if (clazz == Byte.class) {
                    return number.shortValue();
                }

                return value;
            }

            if (value instanceof String) {
                String string = (String) value;

                try {
                    return Short.parseShort(string);
                } catch (NumberFormatException e1) {
                    try {
                        return Long.parseLong(string);
                    } catch (NumberFormatException e2) {
                        double parsedDouble = Double.parseDouble(string);

                        short shortValue = (short) parsedDouble;
                        if (Numbers.equalDoubles(parsedDouble, (double) shortValue)) {
                            return shortValue;
                        }

                        return parsedDouble;
                    }
                }
            }

            throw new IllegalArgumentException("Cannot convert [" + value + "] to number");
        }

    }

    static class BooleanConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof Boolean) {
                return value;
            }
            if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.intValue() != 0;
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to boolean");
        }

    }

    static class ByteConverter extends BaseTypeConverter {

        @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:returncount"})
        @Override
        Comparable convertInternal(Comparable value) {
            Class clazz = value.getClass();

            if (clazz == Byte.class) {
                return value;
            }

            if (value instanceof Number) {
                Number number = (Number) value;

                if (clazz == Long.class) {
                    byte byteValue = number.byteValue();
                    if (number.longValue() == (long) byteValue) {
                        return byteValue;
                    }
                } else if (clazz == Double.class) {
                    byte byteValue = number.byteValue();
                    if (Numbers.equalDoubles(number.doubleValue(), (double) byteValue)) {
                        return byteValue;
                    }
                } else if (clazz == Integer.class) {
                    byte byteValue = number.byteValue();
                    if (number.intValue() == (int) byteValue) {
                        return byteValue;
                    }
                } else if (clazz == Float.class) {
                    byte byteValue = number.byteValue();
                    if (Numbers.equalFloats(number.floatValue(), (float) byteValue)) {
                        return byteValue;
                    }
                } else if (clazz == Short.class) {
                    byte byteValue = number.byteValue();
                    if (number.shortValue() == (short) byteValue) {
                        return byteValue;
                    }
                }

                return value;
            }

            if (value instanceof String) {
                String string = (String) value;

                try {
                    return Byte.parseByte(string);
                } catch (NumberFormatException e1) {
                    try {
                        return Long.parseLong(string);
                    } catch (NumberFormatException e2) {
                        double parsedDouble = Double.parseDouble(string);

                        byte byteValue = (byte) parsedDouble;
                        if (Numbers.equalDoubles(parsedDouble, (double) byteValue)) {
                            return byteValue;
                        }

                        return parsedDouble;
                    }
                }
            }

            throw new IllegalArgumentException("Cannot convert [" + value + "] to number");
        }

    }

    static class CharConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value.getClass() == char.class) {
                return value;
            }
            if (value instanceof Character) {
                return value;
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return (char) number.intValue();
            }
            if (value instanceof String) {
                String string = (String) value;
                if (string.length() == 1) {
                    return string.charAt(0);
                }
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to char");
        }

    }

    static class PortableConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof Portable) {
                return value;
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "]");
        }

    }

    /**
     * @see com.hazelcast.sql.SqlColumnType TIME
     */
    static class SqlLocalTimeConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof LocalTime) {
                return value;
            }
            if (value instanceof String) {
                return LocalTime.parse((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return convertNumberToLocalTime(number);
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to java.time.LocalTime");
        }

        private LocalTime convertNumberToLocalTime(Number number) {
            LocalDateTime ldTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(number.longValue()), TimeZone.getDefault().toZoneId());
            return ldTime.toLocalTime();
        }
    }

    /**
     * @see com.hazelcast.sql.SqlColumnType DATE
     */
    static class SqlLocalDateConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof LocalDate) {
                return value;
            }
            if (value instanceof String) {
                return LocalDate.parse((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return convertNumberToLocalTime(number);
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to java.time.LocalTime");
        }

        private LocalDate convertNumberToLocalTime(Number number) {
            LocalDateTime ldTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(number.longValue()), TimeZone.getDefault().toZoneId());
            return ldTime.toLocalDate();
        }
    }

    /**
     * @see com.hazelcast.sql.SqlColumnType TIMESTAMP
     */
    static class SqlLocalDateTimeConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof LocalDateTime) {
                return value;
            }
            if (value instanceof String) {
                return LocalDateTime.parse((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return convertNumberToLocalDateTime(number);
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to java.time.LocalDateTime");
        }

        private LocalDateTime convertNumberToLocalDateTime(Number number) {
            return LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(number.longValue()), TimeZone.getDefault().toZoneId());
        }
    }

    /**
     * @see com.hazelcast.sql.SqlColumnType TIMESTAMP_WITH_TIME_ZONE
     */
    static class SqlOffsetDateTimeConverter extends BaseTypeConverter {

        @Override
        Comparable convertInternal(Comparable value) {
            if (value instanceof OffsetDateTime) {
                return value;
            }
            if (value instanceof String) {
                return OffsetDateTime.parse((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return convertNumberToOffsetDateTime(number);
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to java.time.OffsetDateTime");
        }

        private OffsetDateTime convertNumberToOffsetDateTime(Number number) {
            return OffsetDateTime.ofInstant(
                    Instant.ofEpochMilli(number.longValue()), TimeZone.getDefault().toZoneId());
        }

    }

}

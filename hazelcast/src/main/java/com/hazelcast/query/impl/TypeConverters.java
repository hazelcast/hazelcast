/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

class TypeConverters {
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

    public interface TypeConverter {
        Comparable convert(Comparable value);
    }

    static class EnumConverter implements TypeConverter {
        public Comparable convert(Comparable value) {
            String valueString = value.toString();
            try {
                if (valueString.contains(".")) {
                    // there is a dot  in the value specifier, keep part after last dot
                    return valueString.substring(1 + valueString.lastIndexOf("."));
                }
            } catch (IllegalArgumentException iae) {
                // illegal enum value specification
                throw new IllegalArgumentException("Illegal enum value specification: " + iae.getMessage());
            }
            return valueString;
        }
    }

    static class SqlDateConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof java.sql.Date) return value;
            if (value instanceof String) {
                return DateHelper.parseSqlDate((String) value);
            }
            Number number = (Number) value;
            return new java.sql.Date(number.longValue());
        }
    }

    static class SqlTimestampConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Timestamp) return value;
            if (value instanceof String) {
                return DateHelper.parseTimeStamp((String) value);
            }
            Number number = (Number) value;
            return new Timestamp(number.longValue());
        }
    }

    static class DateConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Date) return value;
            if (value instanceof String) {
                return DateHelper.parseDate((String) value);
            }
            Number number = (Number) value;
            return new Date(number.longValue());
        }
    }

    static class DoubleConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Double) return value;
            if (value instanceof String) {
                return Double.parseDouble((String) value);
            }
            Number number = (Number) value;
            return number.doubleValue();
        }
    }

    static class LongConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Long) return value;
            if (value instanceof String) {
                return Long.parseLong((String) value);
            }
            Number number = (Number) value;
            return number.longValue();
        }
    }

    static class BigIntegerConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof BigInteger) return value;
            return new BigInteger(value.toString());
        }
    }

    static class BigDecimalConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof BigDecimal) return value;
            if (value instanceof BigInteger) return new BigDecimal((BigInteger) value);
            return new BigDecimal(value.toString());
        }
    }

    static class IntegerConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Integer) return value;
            if (value instanceof String) {
                return Integer.parseInt((String) value);
            }
            Number number = (Number) value;
            return number.intValue();
        }
    }

    static class StringConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof String) return value;
            if (value == null) return IndexImpl.NULL;
            return value.toString();
        }
    }

    static class FloatConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Float) return value;
            if (value instanceof String) {
                return Float.parseFloat((String) value);
            }
            Number number = (Number) value;
            return number.floatValue();
        }
    }

    static class ShortConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Short) return value;
            if (value instanceof String) {
                return Short.parseShort((String) value);
            }
            Number number = (Number) value;
            return number.shortValue();
        }
    }

    static class BooleanConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Boolean) return value;
            if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            }
            Number number = (Number) value;
            return number.intValue() == 1 ? true : false;
        }
    }

    static class ByteConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Byte) return value;
            if (value instanceof String) {
                return Byte.parseByte((String) value);
            }
            Number number = (Number) value;
            return number.byteValue();
        }
    }

    static class CharConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value.getClass() == char.class) return value;
            if (value instanceof Character) return value;
            if (value instanceof String) {
                return ((String) value).charAt(0);
            }
            Number number = (Number) value;
            return number.intValue();
        }
    }
}

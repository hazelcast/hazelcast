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

final class TypeConverters {
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

    private TypeConverters() {
    }

    public interface TypeConverter {
        Comparable convert(Comparable value);
    }

    static class EnumConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value == null) {
                return null;
            }
            String enumString = value.toString();
            if (enumString.contains(".")) {
                // there is a dot  in the value specifier, keep part after last dot
                enumString = enumString.substring(1 + enumString.lastIndexOf('.'));
            }
            return enumString;
        }
    }

    static class SqlDateConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof java.sql.Date) {
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

    static class SqlTimestampConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof Timestamp) {
                return value;
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

    static class DateConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
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

    static class DoubleConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof Double) {
                return value;
            }
            if (value instanceof String) {
                return Double.parseDouble((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.doubleValue();
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to double");
        }
    }

    static class LongConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof Long) {
                return value;
            }
            if (value instanceof String) {
                return Long.parseLong((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.longValue();
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to long");
        }
    }

    static class BigIntegerConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof BigInteger) {
                return value;
            }
            return new BigInteger(value.toString());
        }
    }

    static class BigDecimalConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof BigDecimal) {
                return value;
            }
            if (value instanceof BigInteger) {
                return new BigDecimal((BigInteger) value);
            }
            return new BigDecimal(value.toString());
        }
    }

    static class IntegerConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof Integer) {
                return value;
            }
            if (value instanceof String) {
                return Integer.parseInt((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.intValue();
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to integer");
        }
    }

    static class StringConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof String) {
                return value;
            }
            if (value == null) {
                return IndexImpl.NULL;
            }
            return value.toString();
        }
    }

    static class FloatConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof Float) {
                return value;
            }
            if (value instanceof String) {
                return Float.parseFloat((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.floatValue();
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to float");
        }
    }

    static class ShortConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof Short) {
                return value;
            }
            if (value instanceof String) {
                return Short.parseShort((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.shortValue();
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to short");
        }
    }

    static class BooleanConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
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

    static class ByteConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof Byte) {
                return value;
            }
            if (value instanceof String) {
                return Byte.parseByte((String) value);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.byteValue();
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to byte");
        }
    }

    static class CharConverter implements TypeConverter {
        @Override
        public Comparable convert(Comparable value) {
            if (value.getClass() == char.class) {
                return value;
            }
            if (value instanceof Character) {
                return value;
            }
            if (value instanceof String) {
                return ((String) value).charAt(0);
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.intValue();
            }
            throw new IllegalArgumentException("Cannot convert [" + value + "] to char");
        }
    }
}

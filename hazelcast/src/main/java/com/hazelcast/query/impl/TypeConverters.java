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

public class TypeConverters {
    public static final TypeConverter DOUBLE_CONVERTER = new DoubleConverter();
    public static final TypeConverter LONG_CONVERTER = new LongConverter();
    public static final TypeConverter INTEGER_CONVERTER = new IntegerConverter();
    public static final TypeConverter BOOLEAN_CONVERTER = new BooleanConverter();
    public static final TypeConverter SHORT_CONVERTER = new ShortConverter();
    public static final TypeConverter FLOAT_CONVERTER = new FloatConverter();
    public static final TypeConverter STRING_CONVERTER = new StringConverter();

    public interface TypeConverter {
        Comparable convert(Comparable value);
    }

    private static class DoubleConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Double) return value;
            if (value instanceof String) {
                return Double.parseDouble((String) value);
            }
            Number number = (Number) value;
            return number.doubleValue();
        }
    }

    private static class LongConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Long) return value;
            if (value instanceof String) {
                return Long.parseLong((String) value);
            }
            Number number = (Number) value;
            return number.longValue();
        }
    }

    private static class IntegerConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Integer) return value;
            if (value instanceof String) {
                return Integer.parseInt((String) value);
            }
            Number number = (Number) value;
            return number.intValue();
        }
    }

    private static class StringConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof String) return value;
            if (value == null) return Index.NULL;
            return value.toString();
        }
    }

    private static class FloatConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Float) return value;
            if (value instanceof String) {
                return Float.parseFloat((String) value);
            }
            Number number = (Number) value;
            return number.floatValue();
        }
    }

    private static class ShortConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Short) return value;
            if (value instanceof String) {
                return Short.parseShort((String) value);
            }
            Number number = (Number) value;
            return number.shortValue();
        }
    }

    private static class BooleanConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (value instanceof Boolean) return value;
            if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            }
            Number number = (Number) value;
            return number.intValue() == 1 ? true : false;
        }
    }
}

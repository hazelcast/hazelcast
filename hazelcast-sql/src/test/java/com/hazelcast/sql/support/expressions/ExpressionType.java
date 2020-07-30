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

package com.hazelcast.sql.support.expressions;

import com.hazelcast.sql.impl.type.QueryDataType;
import org.omg.CORBA.ObjectHolder;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class ExpressionType<T> {

    public abstract String typeName();
    public abstract List<T> values();
    public abstract T valueFrom();
    public abstract T valueTo();
    // TODO: Move to-literal conversion to index tests
    public abstract String toLiteral(Object value);
    public abstract QueryDataType getFieldConverterType();

    // TODO: Move parameter variations to index tests
    public Set<Object> parameterVariations(Object value) {
        if (value == null) {
            return Collections.singleton(null);
        }

        return parameterVariationsNotNull(value);
    }

    protected abstract Set<Object> parameterVariationsNotNull(Object value);

    @Override
    public String toString() {
        return typeName();
    }

    public static class BooleanType extends ExpressionType<Boolean>  {
        @Override
        public String typeName() {
            return "Boolean";
        }

        @Override
        public List<Boolean> values() {
            return Arrays.asList(true, false, null);
        }

        @Override
        public Boolean valueFrom() {
            return false;
        }

        @Override
        public Boolean valueTo() {
            return true;
        }

        @Override
        public String toLiteral(Object value) {
            return Boolean.toString((Boolean) value);
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            return new HashSet<>(Arrays.asList(
                value,
                value.toString()
            ));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.BOOLEAN;
        }
    }

    public static class ByteType extends ExpressionType<Byte> {
        @Override
        public String typeName() {
            return "Byte";
        }

        @Override
        public List<Byte> values() {
            return Arrays.asList((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, null);
        }

        @Override
        public Byte valueFrom() {
            return (byte) 2;
        }

        @Override
        public Byte valueTo() {
            return (byte) 4;
        }

        @Override
        public String toLiteral(Object value) {
            return Byte.toString((Byte) value);
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            byte value0 = (byte) value;

            return new HashSet<>(Arrays.asList(
                value0,
                (short) value0,
                (int) value0,
                (long) value0,
                (float) value0,
                (double) value0,
                new BigInteger(Byte.toString(value0)),
                new BigDecimal(Byte.toString(value0)),
                Byte.toString(value0)
            ));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.TINYINT;
        }
    }

    public static class ShortType extends ExpressionType<Short> {
        @Override
        public String typeName() {
            return "Short";
        }

        @Override
        public List<Short> values() {
            return Arrays.asList((short) 1, (short) 2, (short) 3, (short) 4, (short) 5, null);
        }

        @Override
        public Short valueFrom() {
            return (short) 2;
        }

        @Override
        public Short valueTo() {
            return (short) 4;
        }

        @Override
        public String toLiteral(Object value) {
            return Short.toString((Short) value);
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            short value0 = (short) value;

            return new HashSet<>(Arrays.asList(
                (byte) value0,
                value0,
                (int) value0,
                (long) value0,
                (float) value0,
                (double) value0,
                new BigInteger(Short.toString(value0)),
                new BigDecimal(Short.toString(value0)),
                Short.toString(value0)
            ));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.SMALLINT;
        }
    }

    public static class IntegerType extends ExpressionType<Integer> {
        @Override
        public String typeName() {
            return "Integer";
        }

        @Override
        public List<Integer> values() {
            return Arrays.asList(1, 2, 3, 4, 5, null);
        }

        @Override
        public Integer valueFrom() {
            return 2;
        }

        @Override
        public Integer valueTo() {
            return 4;
        }

        @Override
        public String toLiteral(Object value) {
            return Integer.toString((Integer) value);
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            int value0 = (int) value;

            return new HashSet<>(Arrays.asList(
                (byte) value0,
                (short) value0,
                value0,
                (long) value0,
                (float) value0,
                (double) value0,
                new BigInteger(Integer.toString(value0)),
                new BigDecimal(Integer.toString(value0)),
                Integer.toString(value0)
            ));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.INT;
        }
    }

    public static class LongType extends ExpressionType<Long> {
        @Override
        public String typeName() {
            return "Long";
        }

        @Override
        public List<Long> values() {
            return Arrays.asList(1L, 2L, 3L, 4L, 5L, null);
        }

        @Override
        public Long valueFrom() {
            return 2L;
        }

        @Override
        public Long valueTo() {
            return 4L;
        }

        @Override
        public String toLiteral(Object value) {
            return Long.toString((Long) value);
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            long value0 = (long) value;

            return new HashSet<>(Arrays.asList(
                (byte) value0,
                (short) value0,
                (int) value0,
                value0,
                (float) value0,
                (double) value0,
                new BigInteger(Long.toString(value0)),
                new BigDecimal(Long.toString(value0)),
                Long.toString(value0)
            ));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.BIGINT;
        }
    }

    public static class BigDecimalType extends ExpressionType<BigDecimal> {
        @Override
        public String typeName() {
            return "BigDecimal";
        }

        @Override
        public List<BigDecimal> values() {
            return Arrays.asList(
                new BigDecimal("1"),
                new BigDecimal("2"),
                new BigDecimal("3"),
                new BigDecimal("4"),
                new BigDecimal("5"),
                null
            );
        }

        @Override
        public BigDecimal valueFrom() {
            return new BigDecimal("2");
        }

        @Override
        public BigDecimal valueTo() {
            return new BigDecimal("4");
        }

        @Override
        public String toLiteral(Object value) {
            return value.toString();
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            BigDecimal value0 = (BigDecimal) value;

            // TODO: float and double fails for HASH non-composite index (see commented)
            //   because the original BigDecimal "2" is not equal to BigDecimal "2.0"

            return new HashSet<>(Arrays.asList(
                value0.byteValue(),
                value0.shortValue(),
                value0.intValue(),
                value0.longValue(),
                // value0.floatValue(),
                // value0.doubleValue(),
                value0.toBigInteger(),
                value0,
                value0.toString()
            ));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.DECIMAL;
        }
    }

    public static class BigIntegerType extends ExpressionType<BigInteger> {
        @Override
        public String typeName() {
            return "BigInteger";
        }

        @Override
        public List<BigInteger> values() {
            return Arrays.asList(
                new BigInteger("1"),
                new BigInteger("2"),
                new BigInteger("3"),
                new BigInteger("4"),
                new BigInteger("5"),
                null
            );
        }

        @Override
        public BigInteger valueFrom() {
            return new BigInteger("2");
        }

        @Override
        public BigInteger valueTo() {
            return new BigInteger("4");
        }

        @Override
        public String toLiteral(Object value) {
            return value.toString();
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            BigInteger value0 = (BigInteger) value;

            return new HashSet<>(Arrays.asList(
                value0.byteValue(),
                value0.shortValue(),
                value0.intValue(),
                value0.longValue(),
                value0.floatValue(),
                value0.doubleValue(),
                value0,
                new BigDecimal(value0),
                value0.toString()
            ));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.DECIMAL_BIG_INTEGER;
        }
    }

    public static class FloatType extends ExpressionType<Float> {
        @Override
        public String typeName() {
            return "Float";
        }

        @Override
        public List<Float> values() {
            return Arrays.asList(1f, 2f, 3f, 4f, 5f, null);
        }

        @Override
        public Float valueFrom() {
            return 2f;
        }

        @Override
        public Float valueTo() {
            return 4f;
        }

        @Override
        public String toLiteral(Object value) {
            return Float.toString((Float) value);
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            float value0 = (float) value;

            return new HashSet<>(Arrays.asList(
                (byte) value0,
                (short) value0,
                (int) value0,
                (long) value0,
                value0,
                (double) value0,
                new BigInteger(Integer.toString((int) value0)),
                new BigDecimal(Float.toString(value0)),
                Float.toString(value0)
            ));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.REAL;
        }
    }

    public static class DoubleType extends ExpressionType<Double> {
        @Override
        public String typeName() {
            return "Double";
        }

        @Override
        public List<Double> values() {
            return Arrays.asList(1d, 2d, 3d, 4d, 5d, null);
        }

        @Override
        public Double valueFrom() {
            return 2d;
        }

        @Override
        public Double valueTo() {
            return 4d;
        }

        @Override
        public String toLiteral(Object value) {
            return Double.toString((Double) value);
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            double value0 = (double) value;

            return new HashSet<>(Arrays.asList(
                (byte) value0,
                (short) value0,
                (int) value0,
                (long) value0,
                (float) value0,
                value0,
                new BigInteger(Integer.toString((int) value0)),
                new BigDecimal(Double.toString(value0)),
                Double.toString(value0)
            ));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.DOUBLE;
        }
    }

    public static class StringType extends ExpressionType<String> {
        @Override
        public String typeName() {
            return "String";
        }

        @Override
        public List<String> values() {
            return Arrays.asList("a", "b", "c", "d", "e", null);
        }

        @Override
        public String valueFrom() {
            return "b";
        }

        @Override
        public String valueTo() {
            return "d";
        }

        @Override
        public String toLiteral(Object value) {
            return "'" + value + "'";
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            return Collections.singleton(value);
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.VARCHAR;
        }
    }

    public static class CharacterType extends ExpressionType<Character> {
        @Override
        public String typeName() {
            return "Character";
        }

        @Override
        public List<Character> values() {
            return Arrays.asList('a', 'b', 'c', 'd', 'e', null);
        }

        @Override
        public Character valueFrom() {
            return 'b';
        }

        @Override
        public Character valueTo() {
            return 'd';
        }

        @Override
        public String toLiteral(Object value) {
            return "'" + value + "'";
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            char value0 = (char) value;

            return new HashSet<>(Arrays.asList(
                value0,
                Character.toString(value0)
            ));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.VARCHAR_CHARACTER;
        }
    }

    public static class ObjectType extends ExpressionType<Object> {
        @Override
        public String typeName() {
            return "Object";
        }

        @Override
        public List<Object> values() {
            return Arrays.asList(
                new ObjectHolder(1),
                new ObjectHolder(2),
                new ObjectHolder(3),
                new ObjectHolder(4),
                new ObjectHolder(5),
                null);
        }

        @Override
        public Object valueFrom() {
            return new ObjectHolder(2);
        }

        @Override
        public Object valueTo() {
            return new ObjectHolder(4);
        }

        @Override
        public String toLiteral(Object value) {
            throw new UnsupportedOperationException("Unsupported");
        }

        @Override
        public Set<Object> parameterVariationsNotNull(Object value) {
            return new HashSet<>(Collections.singletonList(value));
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.OBJECT;
        }
    }

    public static final class ObjectHolder implements Comparable<ObjectHolder>, Serializable {

        private final int value;

        public ObjectHolder(int value) {
            this.value = value;
        }

        @Override
        public int compareTo(ObjectHolder o) {
            return Integer.compare(value, o.value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ObjectHolder that = (ObjectHolder) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return value;
        }

        @Override
        public String toString() {
            return "[" + value + "]";
        }
    }
}

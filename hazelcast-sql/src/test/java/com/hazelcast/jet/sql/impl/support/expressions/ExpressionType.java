/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.support.expressions;

import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class ExpressionType<T> {

    public abstract String typeName();
    public abstract List<T> values();
    public abstract T valueFrom();
    public abstract T valueMiddle();
    public abstract T valueTo();
    public abstract QueryDataType getFieldConverterType();

    public final List<T> nonNullValues() {
        return values().stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return typeName();
    }

    public static class BooleanType extends ExpressionType<Boolean> {
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
        public Boolean valueMiddle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Boolean valueTo() {
            return true;
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
        public Byte valueMiddle() {
            return (byte) 3;
        }

        @Override
        public Byte valueTo() {
            return (byte) 4;
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
        public Short valueMiddle() {
            return (short) 3;
        }

        @Override
        public Short valueTo() {
            return (short) 4;
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
        public Integer valueMiddle() {
            return 3;
        }

        @Override
        public Integer valueTo() {
            return 4;
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
        public Long valueMiddle() {
            return 3L;
        }

        @Override
        public Long valueTo() {
            return 4L;
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
        public BigDecimal valueMiddle() {
            return new BigDecimal("3");
        }

        @Override
        public BigDecimal valueTo() {
            return new BigDecimal("4");
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
        public BigInteger valueMiddle() {
            return new BigInteger("3");
        }

        @Override
        public BigInteger valueTo() {
            return new BigInteger("4");
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
        public Float valueMiddle() {
            return 3f;
        }

        @Override
        public Float valueTo() {
            return 4f;
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
        public Double valueMiddle() {
            return 3d;
        }

        @Override
        public Double valueTo() {
            return 4d;
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
        public String valueMiddle() {
            return "c";
        }

        @Override
        public String valueTo() {
            return "d";
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
        public Character valueMiddle() {
            return 'c';
        }

        @Override
        public Character valueTo() {
            return 'd';
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.VARCHAR_CHARACTER;
        }
    }

    public static class LocalTimeType extends ExpressionType<LocalTime> {
        @Override
        public String typeName() {
            return "LocalTime";
        }

        @Override
        public List<LocalTime> values() {
            return Arrays.asList(
                    LocalTime.parse("01:00"),
                    LocalTime.parse("02:00"),
                    LocalTime.parse("03:00"),
                    LocalTime.parse("04:00"),
                    LocalTime.parse("05:00"),
                    null);
        }

        @Override
        public LocalTime valueFrom() {
            return LocalTime.parse("02:00");
        }

        @Override
        public LocalTime valueMiddle() {
            return LocalTime.parse("03:00");
        }

        @Override
        public LocalTime valueTo() {
            return LocalTime.parse("04:00");
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.TIME;
        }
    }

    public static class LocalDateType extends ExpressionType<LocalDate> {
        @Override
        public String typeName() {
            return "LocalDate";
        }

        @Override
        public List<LocalDate> values() {
            return Arrays.asList(
                    LocalDate.parse("2020-01-01"),
                    LocalDate.parse("2020-01-02"),
                    LocalDate.parse("2020-01-03"),
                    LocalDate.parse("2020-01-04"),
                    LocalDate.parse("2020-01-05"),
                    null);
        }

        @Override
        public LocalDate valueFrom() {
            return LocalDate.parse("2020-01-02");
        }

        @Override
        public LocalDate valueMiddle() {
            return LocalDate.parse("2020-01-03");
        }

        @Override
        public LocalDate valueTo() {
            return LocalDate.parse("2020-01-04");
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.DATE;
        }
    }

    public static class LocalDateTimeType extends ExpressionType<LocalDateTime> {
        @Override
        public String typeName() {
            return "LocalDateTime";
        }

        @Override
        public List<LocalDateTime> values() {
            return Arrays.asList(
                    LocalDateTime.parse("2020-01-01T00:00:00"),
                    LocalDateTime.parse("2020-01-02T00:00:00"),
                    LocalDateTime.parse("2020-01-03T00:00:00"),
                    LocalDateTime.parse("2020-01-04T00:00:00"),
                    LocalDateTime.parse("2020-01-05T00:00:00"),
                    null);
        }

        @Override
        public LocalDateTime valueFrom() {
            return LocalDateTime.parse("2020-01-02T00:00:00");
        }

        @Override
        public LocalDateTime valueMiddle() {
            return LocalDateTime.parse("2020-01-03T00:00:00");
        }

        @Override
        public LocalDateTime valueTo() {
            return LocalDateTime.parse("2020-01-04T00:00:00");
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.TIMESTAMP;
        }
    }

    public static class OffsetDateTimeType extends ExpressionType<OffsetDateTime> {
        @Override
        public String typeName() {
            return "OffsetDateTime";
        }

        @Override
        public List<OffsetDateTime> values() {
            return Arrays.asList(
                    OffsetDateTime.parse("2020-01-01T00:00:00+00:00"),
                    OffsetDateTime.parse("2020-01-02T00:00:00+00:00"),
                    OffsetDateTime.parse("2020-01-03T00:00:00+00:00"),
                    OffsetDateTime.parse("2020-01-04T00:00:00+00:00"),
                    OffsetDateTime.parse("2020-01-05T00:00:00+00:00"),
                    null);
        }

        @Override
        public OffsetDateTime valueFrom() {
            return OffsetDateTime.parse("2020-01-02T00:00:00+00:00");
        }

        @Override
        public OffsetDateTime valueMiddle() {
            return OffsetDateTime.parse("2020-01-03T00:00:00+00:00");
        }

        @Override
        public OffsetDateTime valueTo() {
            return OffsetDateTime.parse("2020-01-04T00:00:00+00:00");
        }

        @Override
        public QueryDataType getFieldConverterType() {
            return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
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
        public Object valueMiddle() {
            return new ObjectHolder(3);
        }

        @Override
        public Object valueTo() {
            return new ObjectHolder(4);
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

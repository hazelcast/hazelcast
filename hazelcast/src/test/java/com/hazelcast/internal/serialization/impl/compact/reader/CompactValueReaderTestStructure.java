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

package com.hazelcast.internal.serialization.impl.compact.reader;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;

public class CompactValueReaderTestStructure {

    public enum PrimitiveFields {
        Byte("byte_"),
        Boolean("boolean_"),
        Short("short_"),
        Int("int_"),
        Long("long_"),
        Float("float_"),
        Double("double_"),
        UTF("string_"),
        BigDecimal("bigDecimal_"),
        LocalTime("localTime_"),
        LocalDate("localDate_"),
        LocalDateTime("localDateTime_"),
        OffsetDateTime("offsetDateTime_"),

        ByteArray("bytes"),
        BooleanArray("booleans"),
        ShortArray("shorts"),
        IntArray("ints"),
        LongArray("longs"),
        FloatArray("floats"),
        DoubleArray("doubles"),
        UTFArray("strings"),
        BigDecimalArray("bigDecimals"),
        LocalTimeArray("localTimes"),
        LocalDateArray("localDates"),
        LocalDateTimeArray("localDateTimes"),
        OffsetDateTimeArray("offsetDateTimes"),

        NullableBoolean("nullableBoolean_"),
        NullableByte("nullableByte_"),
        NullableShort("nullableShort_"),
        NullableInteger("nullableInt_"),
        NullableLong("nullableLong_"),
        NullableFloat("nullableFloat_"),
        NullableDouble("nullableDouble_"),
        NullableBooleanArray("nullableBooleans"),
        NullableByteArray("nullableBytes"),
        NullableShortArray("nullableShorts"),
        NullableIntegerArray("nullableInts"),
        NullableLongArray("nullableLongs"),
        NullableFloatArray("nullableFloats"),
        NullableDoubleArray("nullableDoubles");

        PrimitiveFields(String field) {
            this.field = field;
        }

        final String field;

        static List<PrimitiveFields> getPrimitives() {
            return asList(Boolean, Byte, Short, Int, Long, Float, Double, UTF, BigDecimal, LocalTime,
                    LocalDate, LocalDateTime, OffsetDateTime, NullableBoolean, NullableByte, NullableShort,
                    NullableInteger, NullableInteger, NullableLong, NullableFloat, NullableDouble);
        }

        static List<PrimitiveFields> getPrimitiveArrays() {
            return asList(BooleanArray, ByteArray, ShortArray, IntArray, LongArray, FloatArray, DoubleArray,
                    UTFArray, BigDecimalArray, LocalTimeArray, LocalDateArray, LocalDateTimeArray, OffsetDateTimeArray,
                    NullableBooleanArray, NullableByteArray, NullableShortArray, NullableIntegerArray, NullableLongArray,
                    NullableFloatArray, NullableDoubleArray
            );
        }

    }

    public static class PrimitiveObject {

        boolean boolean_;
        byte byte_;
        short short_;
        int int_;
        long long_;
        float float_;
        double double_;
        String string_;
        BigDecimal bigDecimal_;
        LocalTime localTime_;
        LocalDate localDate_;
        LocalDateTime localDateTime_;
        OffsetDateTime offsetDateTime_;

        boolean[] booleans;
        byte[] bytes;
        short[] shorts;
        int[] ints;
        long[] longs;
        float[] floats;
        double[] doubles;
        String[] strings;
        BigDecimal[] bigDecimals;
        LocalTime[] localTimes;
        LocalDate[] localDates;
        LocalDateTime[] localDateTimes;
        OffsetDateTime[] offsetDateTimes;
        Boolean nullableBoolean_;
        Byte nullableByte_;
        Short nullableShort_;
        Integer nullableInt_;
        Long nullableLong_;
        Float nullableFloat_;
        Double nullableDouble_;
        Boolean[] nullableBooleans;
        Byte[] nullableBytes;
        Short[] nullableShorts;
        Integer[] nullableInts;
        Long[] nullableLongs;
        Float[] nullableFloats;
        Double[] nullableDoubles;

        enum Init {
            FULL, NONE, NULL
        }

        int getSeed() {
            return byte_ - 10;
        }

        PrimitiveObject(int seed, Init init) {
            boolean_ = seed % 2 == 0;
            byte_ = (byte) (seed + 10);
            short_ = (short) (seed + 20);
            int_ = seed + 30;
            long_ = seed + 40;
            float_ = seed + 50.01f;
            double_ = seed + 60.01d;

            Random rnd = new Random(seed);
            if (init == Init.FULL) {
                bytes = new byte[]{(byte) (seed + 11), (byte) (seed + 12), (byte) (seed + 13)};
                booleans = new boolean[]{seed % 2 == 0, seed % 2 == 1, seed % 2 == 0};
                shorts = new short[]{(short) (seed + 21), (short) (seed + 22), (short) (seed + 23)};
                ints = new int[]{seed + 31, seed + 32, seed + 33};
                longs = new long[]{seed + 41, seed + 42, seed + 43};
                floats = new float[]{seed + 51.01f, seed + 52.01f, seed + 53.01f};
                doubles = new double[]{seed + 61.01f, seed + 62.01f, seed + 63.01f};
                strings = new String[]{seed + 81 + "text", seed + 82 + "text", seed + 83 + "text"};
                localTime_ = LocalTime.of(17, 41, 47, 874000000);
                localDate_ = LocalDate.of(1, 1, 1);
                localDateTime_ = LocalDateTime.of(localDate_, localTime_);
                offsetDateTime_ = OffsetDateTime.of(localDateTime_, ZoneOffset.ofHours(2));

                string_ = seed + 80 + "text";
                localTime_ = LocalTime.of(17, 41, 47, 874000000);
                localDate_ = LocalDate.of(1, 1, 1);
                localDateTime_ = LocalDateTime.of(localDate_, localTime_);
                offsetDateTime_ = OffsetDateTime.of(localDateTime_, ZoneOffset.ofHours(2));

                bigDecimals = new BigDecimal[]{new BigDecimal(new BigInteger(32, rnd), 2),
                        new BigDecimal(new BigInteger(32, rnd), 2),
                        new BigDecimal(new BigInteger(32, rnd), 2)};
                localTimes = new LocalTime[]{LocalTime.of(1, seed % 60, 0),
                        LocalTime.of(2, seed % 60, 0), LocalTime.of(3, seed % 60, 0)};
                localDates = new LocalDate[]{LocalDate.of(1990, (seed + 1) % 12, 1),
                        LocalDate.of(1990, (seed + 1) % 12, 1),
                        LocalDate.of(1990, (seed + 1) % 12, 1)};
                localDateTimes = new LocalDateTime[]{LocalDateTime.of(localDates[0], localTimes[0]),
                        LocalDateTime.of(localDates[1], localTimes[1]), LocalDateTime.of(localDates[2], localTimes[2])};
                offsetDateTimes = new OffsetDateTime[]{
                        OffsetDateTime.of(localDates[0], localTimes[0], ZoneOffset.ofHours(seed % 18)),
                        OffsetDateTime.of(localDates[1], localTimes[1], ZoneOffset.ofHours(seed % 18)),
                        OffsetDateTime.of(localDates[2], localTimes[2], ZoneOffset.ofHours(seed % 18))};

                nullableBoolean_ = boolean_;
                nullableByte_ = byte_;
                nullableShort_ = short_;
                nullableInt_ = int_;
                nullableLong_ = long_;
                nullableFloat_ = float_;
                nullableDouble_ = double_;

                nullableBytes = new Byte[]{(byte) (seed + 11), (byte) (seed + 12), (byte) (seed + 13), null};
                nullableBooleans = new Boolean[]{seed % 2 == 0, seed % 2 == 1, seed % 2 == 0, null};
                nullableShorts = new Short[]{(short) (seed + 21), (short) (seed + 22), (short) (seed + 23), null};
                nullableInts = new Integer[]{null, seed + 31, seed + 32, seed + 33};
                nullableLongs = new Long[]{(long) (seed + 41), null, (long) (seed + 42), (long) (seed + 43)};
                nullableFloats = new Float[]{seed + 51.01f, seed + 52.01f, null, seed + 53.01f};
                nullableDoubles = new Double[]{null, (double) (seed + 61.01f), (double) (seed + 62.01f), (double) (seed + 63.01f)};

            } else if (init == Init.NONE) {
                bytes = new byte[]{};
                shorts = new short[]{};
                ints = new int[]{};
                longs = new long[]{};
                floats = new float[]{};
                doubles = new double[]{};
                booleans = new boolean[]{};
                strings = new String[]{};
                bigDecimals = new BigDecimal[]{};
                localTimes = new LocalTime[]{};
                localDates = new LocalDate[]{};
                localDateTimes = new LocalDateTime[]{};
                offsetDateTimes = new OffsetDateTime[]{};


                bigDecimal_ = new BigDecimal(new BigInteger(32, rnd), 2);
                localTime_ = LocalTime.of(1, seed % 60, 0);
                localDate_ = LocalDate.of(1990, (seed + 1) % 12, 1);
                localDateTime_ = LocalDateTime.of(localDate_, localTime_);
                offsetDateTime_ = OffsetDateTime.of(localDate_, localTime_, ZoneOffset.ofHours(seed % 18));
                string_ = "";
            }
        }

        PrimitiveObject() {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PrimitiveObject that = (PrimitiveObject) o;
            return byte_ == that.byte_;
        }

        @Override
        public int hashCode() {
            return (int) byte_;
        }

        @Override
        public String toString() {
            String init;
            if (bytes == null) {
                init = "NULL";
            } else if (bytes.length == 0) {
                init = "EMPTY";
            } else {
                init = "FULL";
            }
            return "primitives{" + getSeed() + ", " + init + "}";
        }

        Object getPrimitive(PrimitiveFields primitiveFields) {
            switch (primitiveFields) {
                case Byte:
                    return byte_;
                case Boolean:
                    return boolean_;
                case Short:
                    return short_;
                case Int:
                    return int_;
                case Long:
                    return long_;
                case Float:
                    return float_;
                case Double:
                    return double_;
                case UTF:
                    return string_;
                case BigDecimal:
                    return bigDecimal_;
                case LocalTime:
                    return localTime_;
                case LocalDate:
                    return localDate_;
                case LocalDateTime:
                    return localDateTime_;
                case OffsetDateTime:
                    return offsetDateTime_;
                case NullableBoolean:
                    return nullableBoolean_;
                case NullableByte:
                    return nullableByte_;
                case NullableShort:
                    return nullableShort_;
                case NullableInteger:
                    return nullableInt_;
                case NullableLong:
                    return nullableLong_;
                case NullableFloat:
                    return nullableFloat_;
                case NullableDouble:
                    return nullableDouble_;
                default:
                    throw new RuntimeException("Unsupported method " + primitiveFields);
            }
        }

        Object getPrimitiveArray(PrimitiveFields primitiveFields) {
            switch (primitiveFields) {
                case Byte:
                    return bytes;
                case Boolean:
                    return booleans;
                case Short:
                    return shorts;
                case Int:
                    return ints;
                case Long:
                    return longs;
                case Float:
                    return floats;
                case Double:
                    return doubles;
                case UTF:
                    return strings;
                case BigDecimal:
                    return bigDecimals;
                case LocalTime:
                    return localTimes;
                case LocalDate:
                    return localDates;
                case LocalDateTime:
                    return localDateTimes;
                case OffsetDateTime:
                    return offsetDateTimes;
                //
                case ByteArray:
                    return bytes;
                case BooleanArray:
                    return booleans;
                case ShortArray:
                    return shorts;
                case IntArray:
                    return ints;
                case LongArray:
                    return longs;
                case FloatArray:
                    return floats;
                case DoubleArray:
                    return doubles;
                case UTFArray:
                    return strings;
                case BigDecimalArray:
                    return bigDecimals;
                case LocalTimeArray:
                    return localTimes;
                case LocalDateArray:
                    return localDates;
                case LocalDateTimeArray:
                    return localDateTimes;
                case OffsetDateTimeArray:
                    return offsetDateTimes;
                case NullableBoolean:
                    return nullableBooleans;
                case NullableByte:
                    return nullableBytes;
                case NullableShort:
                    return nullableShorts;
                case NullableInteger:
                    return nullableInts;
                case NullableLong:
                    return nullableLongs;
                case NullableFloat:
                    return nullableFloats;
                case NullableDouble:
                    return nullableDoubles;
                case NullableBooleanArray:
                    return nullableBooleans;
                case NullableByteArray:
                    return nullableBytes;
                case NullableShortArray:
                    return nullableShorts;
                case NullableIntegerArray:
                    return nullableInts;
                case NullableLongArray:
                    return nullableLongs;
                case NullableFloatArray:
                    return nullableFloats;
                case NullableDoubleArray:
                    return nullableDoubles;
                default:
                    throw new RuntimeException("Unsupported array method " + primitiveFields);
            }
        }
    }

    static class GroupObject {


        PrimitiveObject object;
        PrimitiveObject[] objects;

        GroupObject() {
        }

        GroupObject(PrimitiveObject object) {
            this.object = object;
        }

        GroupObject(PrimitiveObject[] objects) {
            if (objects != null && objects.length > 0) {
                this.object = objects[0];
            }
            this.objects = objects;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupObject that = (GroupObject) o;
            return Arrays.equals(objects, that.objects);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(objects);
        }

        @Override
        public String toString() {
            String result = "";
            if (object != null) {
                result += "object=" + object.toString() + ", ";
            } else {
                result += "object=null, ";
            }
            if (objects != null) {
                result += "objects=";
                for (Object object : objects) {
                    result += object.toString() + ", ";
                }
            }
            if (object == null && objects == null) {
                result += "objects=null";
            }
            if (object == null && objects != null && objects.length == 0) {
                result += "objects.length==0";
            }
            return "GroupObject{" + result + "}";
        }
    }

    static class NestedGroupObject {

        GroupObject object;
        GroupObject[] objects;

        NestedGroupObject() {
        }

        NestedGroupObject(GroupObject object) {
            this.object = object;
            this.objects = new GroupObject[]{object};
        }

        NestedGroupObject(GroupObject[] objects) {
            if (objects != null && objects.length > 0) {
                this.object = objects[0];
            }
            this.objects = objects;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupObject that = (GroupObject) o;
            return Arrays.equals(objects, that.objects);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(objects);
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            if (object != null) {
                result.append("object=").append(object.toString()).append(", ");
            } else {
                result.append("object=null, ");
            }
            if (objects != null) {
                result.append("objects=");
                for (Object object : objects) {
                    result.append(object.toString()).append(", ");
                }
            }
            if (object == null && objects == null) {
                result.append("objects=null");
            }
            if (object == null && objects != null && objects.length == 0) {
                result.append("objects.length==0");
            }
            return "NestedGroupObject{" + result + "}";
        }
    }

    static GroupObject group(PrimitiveObject.Init init) {
        PrimitiveObject[] objects;
        if (init == PrimitiveObject.Init.FULL) {
            int count = 3;
            objects = new PrimitiveObject[count];
            for (int i = 0; i < count; i++) {
                objects[i] = new PrimitiveObject(i, PrimitiveObject.Init.FULL);
            }
        } else if (init == PrimitiveObject.Init.NULL) {
            objects = null;
        } else {
            objects = new PrimitiveObject[0];
        }
        return new GroupObject(objects);
    }

    static GroupObject group(PrimitiveObject object) {
        return new GroupObject(object);
    }

    static NestedGroupObject nested(GroupObject object) {
        return new NestedGroupObject(object);
    }

    static NestedGroupObject nested(GroupObject[] objects) {
        return new NestedGroupObject(objects);
    }

    static GroupObject group(PrimitiveObject... objects) {
        return new GroupObject(objects);
    }

    static PrimitiveObject prim(PrimitiveObject.Init init) {
        return new PrimitiveObject(1, init);
    }

    static PrimitiveObject prim(int seed, PrimitiveObject.Init init) {
        return new PrimitiveObject(seed, init);
    }
}

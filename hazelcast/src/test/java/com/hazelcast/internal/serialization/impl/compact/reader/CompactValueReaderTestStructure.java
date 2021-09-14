/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
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
        UnsignedByte("unsignedByte_"),
        Boolean("boolean_"),
        Char("char_"),
        Short("short_"),
        UnsignedShort("unsignedShort_"),
        Int("int_"),
        UnsignedInt("unsignedInt_"),
        Long("long_"),
        UnsignedLong("unsignedLong_"),
        Float("float_"),
        Double("double_"),
        UTF("string_"),
        BigDecimal("bigDecimal_"),
        LocalTime("localTime_"),
        LocalDate("localDate_"),
        LocalDateTime("localDateTime_"),
        OffsetDateTime("offsetDateTime_"),

        ByteArray("bytes"),
        UnsignedByteArray("unsignedBytes"),
        BooleanArray("booleans"),
        CharArray("chars"),
        ShortArray("shorts"),
        UnsignedShortArray("unsignedShorts"),
        IntArray("ints"),
        UnsignedIntArray("unsignedInts"),
        LongArray("longs"),
        UnsignedLongArray("unsignedLongs"),
        FloatArray("floats"),
        DoubleArray("doubles"),
        UTFArray("strings"),
        BigDecimalArray("bigDecimals"),
        LocalTimeArray("localTimes"),
        LocalDateArray("localDates"),
        LocalDateTimeArray("localDateTimes"),
        OffsetDateTimeArray("offsetDateTimes");

        PrimitiveFields(String field) {
            this.field = field;
        }

        final String field;

        static List<PrimitiveFields> getPrimitives() {
            return asList(Byte, UnsignedByte, Boolean, Char, Short, UnsignedShort, Int, UnsignedInt, Long, UnsignedLong,
                    Float, Double, UTF, BigDecimal, LocalTime, LocalDate, LocalDateTime, OffsetDateTime);
        }

        static List<PrimitiveFields> getPrimitiveArrays() {
            return asList(ByteArray, UnsignedByteArray, BooleanArray, CharArray, ShortArray, UnsignedShortArray,
                    IntArray, UnsignedIntArray, LongArray, UnsignedLongArray, FloatArray, DoubleArray, UTFArray,
                    BigDecimalArray, LocalTimeArray, LocalDateArray, LocalDateTimeArray, OffsetDateTimeArray);
        }

    }

    public static class PrimitiveObject {

        byte byte_;
        int unsignedByte_;
        short short_;
        int unsignedShort_;
        int int_;
        long unsignedInt_;
        long long_;
        BigInteger unsignedLong_;
        float float_;
        double double_;
        boolean boolean_;
        char char_;
        String string_;
        BigDecimal bigDecimal_;
        LocalTime localTime_;
        LocalDate localDate_;
        LocalDateTime localDateTime_;
        OffsetDateTime offsetDateTime_;

        byte[] bytes;
        int[] unsignedBytes;
        short[] shorts;
        int[] unsignedShorts;
        int[] ints;
        long[] unsignedInts;
        long[] longs;
        BigInteger[] unsignedLongs;
        float[] floats;
        double[] doubles;
        boolean[] booleans;
        char[] chars;
        String[] strings;
        BigDecimal[] bigDecimals;
        LocalTime[] localTimes;
        LocalDate[] localDates;
        LocalDateTime[] localDateTimes;
        OffsetDateTime[] offsetDateTimes;

        enum Init {
            FULL, NONE, NULL
        }

        public static class PrimitiveObjectSerializer implements CompactSerializer<PrimitiveObject> {
            @NotNull
            @Override
            public PrimitiveObject read(@NotNull CompactReader in) throws IOException {
                PrimitiveObject o = new PrimitiveObject();
                o.byte_ = in.readByte(PrimitiveFields.Byte.field);
                o.unsignedByte_ = in.readUnsignedByte(PrimitiveFields.UnsignedByte.field);
                o.short_ = in.readShort(PrimitiveFields.Short.field);
                o.unsignedShort_ = in.readUnsignedShort(PrimitiveFields.UnsignedShort.field);
                o.int_ = in.readInt(PrimitiveFields.Int.field);
                o.unsignedInt_ = in.readUnsignedInt(PrimitiveFields.UnsignedInt.field);
                o.long_ = in.readLong(PrimitiveFields.Long.field);
                o.unsignedLong_ = in.readUnsignedLong(PrimitiveFields.UnsignedLong.field);
                o.float_ = in.readFloat(PrimitiveFields.Float.field);
                o.double_ = in.readDouble(PrimitiveFields.Double.field);
                o.boolean_ = in.readBoolean(PrimitiveFields.Boolean.field);
                o.char_ = in.readChar(PrimitiveFields.Char.field);
                o.string_ = in.readString(PrimitiveFields.UTF.field);
                o.bigDecimal_ = in.readDecimal(PrimitiveFields.BigDecimal.field);
                o.localDate_ = in.readDate(PrimitiveFields.LocalDate.field);
                o.localTime_ = in.readTime(PrimitiveFields.LocalTime.field);
                o.localDateTime_ = in.readTimestamp(PrimitiveFields.LocalDateTime.field);
                o.offsetDateTime_ = in.readTimestampWithTimezone(PrimitiveFields.OffsetDateTime.field);

                o.bytes = in.readByteArray(PrimitiveFields.ByteArray.field);
                o.unsignedBytes = in.readUnsignedByteArray(PrimitiveFields.UnsignedByteArray.field);
                o.shorts = in.readShortArray(PrimitiveFields.ShortArray.field);
                o.unsignedShorts = in.readUnsignedShortArray(PrimitiveFields.UnsignedShortArray.field);
                o.ints = in.readIntArray(PrimitiveFields.IntArray.field);
                o.unsignedInts = in.readUnsignedIntArray(PrimitiveFields.UnsignedIntArray.field);
                o.longs = in.readLongArray(PrimitiveFields.LongArray.field);
                o.unsignedLongs = in.readUnsignedLongArray(PrimitiveFields.UnsignedLongArray.field);
                o.floats = in.readFloatArray(PrimitiveFields.FloatArray.field);
                o.doubles = in.readDoubleArray(PrimitiveFields.DoubleArray.field);
                o.booleans = in.readBooleanArray(PrimitiveFields.BooleanArray.field);
                o.chars = in.readCharArray(PrimitiveFields.CharArray.field);
                o.strings = in.readStringArray(PrimitiveFields.UTFArray.field);
                o.bigDecimals = in.readDecimalArray(PrimitiveFields.BigDecimalArray.field);
                o.localDates = in.readDateArray(PrimitiveFields.LocalDateArray.field);
                o.localTimes = in.readTimeArray(PrimitiveFields.LocalTimeArray.field);
                o.localDateTimes = in.readTimestampArray(PrimitiveFields.LocalDateTimeArray.field);
                o.offsetDateTimes = in.readTimestampWithTimezoneArray(PrimitiveFields.OffsetDateTimeArray.field);

                return o;
            }

            @Override
            public void write(@NotNull CompactWriter out, @NotNull CompactValueReaderTestStructure.PrimitiveObject o) throws IOException {
                out.writeByte(PrimitiveFields.Byte.field, o.byte_);
                out.writeUnsignedByte(PrimitiveFields.UnsignedByte.field, o.unsignedByte_);
                out.writeChar(PrimitiveFields.Char.field, o.char_);
                out.writeString(PrimitiveFields.UTF.field, o.string_);
                out.writeDecimal(PrimitiveFields.BigDecimal.field, o.bigDecimal_);
                out.writeBoolean(PrimitiveFields.Boolean.field, o.boolean_);
                out.writeShort(PrimitiveFields.Short.field, o.short_);
                out.writeUnsignedShort(PrimitiveFields.UnsignedShort.field, o.unsignedShort_);
                out.writeInt(PrimitiveFields.Int.field, o.int_);
                out.writeUnsignedInt(PrimitiveFields.UnsignedInt.field, o.unsignedInt_);
                out.writeLong(PrimitiveFields.Long.field, o.long_);
                out.writeUnsignedLong(PrimitiveFields.UnsignedLong.field, o.unsignedLong_);
                out.writeFloat(PrimitiveFields.Float.field, o.float_);
                out.writeDouble(PrimitiveFields.Double.field, o.double_);
                out.writeDate(PrimitiveFields.LocalDate.field, o.localDate_);
                out.writeTime(PrimitiveFields.LocalTime.field, o.localTime_);
                out.writeTimestamp(PrimitiveFields.LocalDateTime.field, o.localDateTime_);
                out.writeTimestampWithTimezone(PrimitiveFields.OffsetDateTime.field, o.offsetDateTime_);

                out.writeByteArray(PrimitiveFields.ByteArray.field, o.bytes);
                out.writeUnsignedByteArray(PrimitiveFields.UnsignedByteArray.field, o.unsignedBytes);
                out.writeCharArray(PrimitiveFields.CharArray.field, o.chars);
                out.writeStringArray(PrimitiveFields.UTFArray.field, o.strings);
                out.writeDecimalArray(PrimitiveFields.BigDecimalArray.field, o.bigDecimals);
                out.writeBooleanArray(PrimitiveFields.BooleanArray.field, o.booleans);
                out.writeShortArray(PrimitiveFields.ShortArray.field, o.shorts);
                out.writeUnsignedShortArray(PrimitiveFields.UnsignedShortArray.field, o.unsignedShorts);
                out.writeIntArray(PrimitiveFields.IntArray.field, o.ints);
                out.writeUnsignedIntArray(PrimitiveFields.UnsignedIntArray.field, o.unsignedInts);
                out.writeLongArray(PrimitiveFields.LongArray.field, o.longs);
                out.writeUnsignedLongArray(PrimitiveFields.UnsignedLongArray.field, o.unsignedLongs);
                out.writeFloatArray(PrimitiveFields.FloatArray.field, o.floats);
                out.writeDoubleArray(PrimitiveFields.DoubleArray.field, o.doubles);
                out.writeDateArray(PrimitiveFields.LocalDateArray.field, o.localDates);
                out.writeTimeArray(PrimitiveFields.LocalTimeArray.field, o.localTimes);
                out.writeTimestampArray(PrimitiveFields.LocalDateTimeArray.field, o.localDateTimes);
                out.writeTimestampWithTimezoneArray(PrimitiveFields.OffsetDateTimeArray.field, o.offsetDateTimes);
            }
        }

        int getSeed() {
            return byte_ - 10;
        }

        PrimitiveObject(int seed, Init init) {
            int positiveSeed = Math.abs(seed);
            byte_ = (byte) (seed + 10);
            unsignedByte_ = positiveSeed + 11;
            short_ = (short) (seed + 20);
            unsignedShort_ = positiveSeed + 20;
            int_ = seed + 30;
            unsignedInt_ = positiveSeed + 30;
            long_ = seed + 40;
            unsignedLong_ = BigInteger.valueOf(positiveSeed + 40);
            float_ = seed + 50.01f;
            double_ = seed + 60.01d;
            boolean_ = seed % 2 == 0;
            char_ = (char) (seed + 'a');
            localTime_ = LocalTime.of(17, 41, 47, 874000000);
            localDate_ = LocalDate.of(1, 1, 1);
            localDateTime_ = LocalDateTime.of(localDate_, localTime_);
            offsetDateTime_ = OffsetDateTime.of(localDateTime_, ZoneOffset.ofHours(2));

            Random rnd = new Random(seed);
            if (init == Init.FULL) {
                bytes = new byte[]{(byte) (seed + 11), (byte) (seed + 12), (byte) (seed + 13)};
                unsignedBytes = new int[]{positiveSeed + 11, positiveSeed + 12, positiveSeed + 13};
                shorts = new short[]{(short) (seed + 21), (short) (seed + 22), (short) (seed + 23)};
                unsignedShorts = new int[]{positiveSeed + 14, positiveSeed + 15, positiveSeed + 16};
                ints = new int[]{seed + 31, seed + 32, seed + 33};
                unsignedInts = new long[]{positiveSeed + 34, positiveSeed + 35, positiveSeed + 36};
                longs = new long[]{seed + 41, seed + 42, seed + 43};
                unsignedLongs = new BigInteger[]{BigInteger.valueOf(positiveSeed + 37),
                        BigInteger.valueOf(positiveSeed + 38), BigInteger.valueOf(positiveSeed + 39)};
                floats = new float[]{seed + 51.01f, seed + 52.01f, seed + 53.01f};
                doubles = new double[]{seed + 61.01f, seed + 62.01f, seed + 63.01f};
                booleans = new boolean[]{seed % 2 == 0, seed % 2 == 1, seed % 2 == 0};
                chars = new char[]{(char) (seed + 'b'), (char) (seed + 'c'), (char) (seed + 'd')};
                strings = new String[]{seed + 81 + "text", seed + 82 + "text", seed + 83 + "text"};

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
            } else if (init == Init.NONE) {
                bytes = new byte[]{};
                unsignedBytes = new int[]{};
                shorts = new short[]{};
                unsignedShorts = new int[]{};
                ints = new int[]{};
                unsignedInts = new long[]{};
                longs = new long[]{};
                unsignedLongs = new BigInteger[]{};
                floats = new float[]{};
                doubles = new double[]{};
                booleans = new boolean[]{};
                chars = new char[]{};
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
                case UnsignedByte:
                    return unsignedByte_;
                case Boolean:
                    return boolean_;
                case Char:
                    return char_;
                case Short:
                    return short_;
                case UnsignedShort:
                    return unsignedShort_;
                case Int:
                    return int_;
                case UnsignedInt:
                    return unsignedInt_;
                case Long:
                    return long_;
                case UnsignedLong:
                    return unsignedLong_;
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
                default:
                    throw new RuntimeException("Unsupported method " + primitiveFields);
            }
        }

        Object getPrimitiveArray(PrimitiveFields primitiveFields) {
            switch (primitiveFields) {
                case Byte:
                case ByteArray:
                    return bytes;
                case UnsignedByte:
                case UnsignedByteArray:
                    return unsignedBytes;
                case Boolean:
                case BooleanArray:
                    return booleans;
                case Char:
                case CharArray:
                    return chars;
                case Short:
                case ShortArray:
                    return shorts;
                case UnsignedShort:
                case UnsignedShortArray:
                    return unsignedShorts;
                case Int:
                case IntArray:
                    return ints;
                case UnsignedInt:
                case UnsignedIntArray:
                    return unsignedInts;
                case Long:
                case LongArray:
                    return longs;
                case UnsignedLong:
                case UnsignedLongArray:
                    return unsignedLongs;
                case Float:
                case FloatArray:
                    return floats;
                case Double:
                case DoubleArray:
                    return doubles;
                case UTF:
                case UTFArray:
                    return strings;
                case BigDecimal:
                case BigDecimalArray:
                    return bigDecimals;
                case LocalTime:
                case LocalTimeArray:
                    return localTimes;
                case LocalDate:
                case LocalDateArray:
                    return localDates;
                case LocalDateTime:
                case LocalDateTimeArray:
                    return localDateTimes;
                case OffsetDateTime:
                case OffsetDateTimeArray:
                    return offsetDateTimes;
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

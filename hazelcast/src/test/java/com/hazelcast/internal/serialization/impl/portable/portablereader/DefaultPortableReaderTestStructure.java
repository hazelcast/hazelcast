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

package com.hazelcast.internal.serialization.impl.portable.portablereader;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

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

public class DefaultPortableReaderTestStructure {

    public enum PrimitiveFields {
        Byte("byte_"),
        Boolean("boolean_"),
        Char("char_"),
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
        CharArray("chars"),
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
        OffsetDateTimeArray("offsetDateTimes");

        PrimitiveFields(String field) {
            this.field = field;
        }

        final String field;

        static List<PrimitiveFields> getPrimitives() {
            return asList(Byte, Boolean, Char, Short, Int, Long, Float, Double, UTF, BigDecimal, LocalTime,
                    LocalDate, LocalDateTime, OffsetDateTime);
        }

        static List<PrimitiveFields> getPrimitiveArrays() {
            return asList(ByteArray, BooleanArray, CharArray, ShortArray, IntArray, LongArray, FloatArray, DoubleArray, UTFArray,
                    BigDecimalArray, LocalTimeArray, LocalDateArray, LocalDateTimeArray, OffsetDateTimeArray);
        }

    }

    public static class PrimitivePortable implements Portable {

        static final int FACTORY_ID = 1;
        static final int ID = 10;

        byte byte_;
        short short_;
        int int_;
        long long_;
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
        short[] shorts;
        int[] ints;
        long[] longs;
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

        int getSeed() {
            return byte_ - 10;
        }

        PrimitivePortable(int seed, Init init) {
            byte_ = (byte) (seed + 10);
            short_ = (short) (seed + 20);
            int_ = seed + 30;
            long_ = seed + 40;
            float_ = seed + 50.01f;
            double_ = seed + 60.01d;
            boolean_ = seed % 2 == 0;
            char_ = (char) (seed + 'a');

            Random rnd = new Random(seed);
            if (init == Init.FULL) {
                bytes = new byte[]{(byte) (seed + 11), (byte) (seed + 12), (byte) (seed + 13)};
                shorts = new short[]{(short) (seed + 21), (short) (seed + 22), (short) (seed + 23)};
                ints = new int[]{seed + 31, seed + 32, seed + 33};
                longs = new long[]{seed + 41, seed + 42, seed + 43};
                floats = new float[]{seed + 51.01f, seed + 52.01f, seed + 53.01f};
                doubles = new double[]{seed + 61.01f, seed + 62.01f, seed + 63.01f};
                booleans = new boolean[]{seed % 2 == 0, seed % 2 == 1, seed % 2 == 0};
                chars = new char[]{(char) (seed + 'b'), (char) (seed + 'c'), (char) (seed + 'd')};
                strings = new String[]{seed + 81 + "text", seed + 82 + "text", seed + 83 + "text"};

                string_ = seed + 80 + "text";
                bigDecimal_ = new BigDecimal(new BigInteger(32, rnd), 2);
                localTime_ = LocalTime.of(1, seed % 60, 0);
                localDate_ = LocalDate.of(1990, (seed + 1) % 12, 1);
                localDateTime_ = LocalDateTime.of(localDate_, localTime_);
                offsetDateTime_ = OffsetDateTime.of(localDate_, localTime_, ZoneOffset.ofHours(seed % 18));

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
                shorts = new short[]{};
                ints = new int[]{};
                longs = new long[]{};
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

        PrimitivePortable() {
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeByte("byte_", byte_);
            writer.writeShort("short_", short_);
            writer.writeInt("int_", int_);
            writer.writeLong("long_", long_);
            writer.writeFloat("float_", float_);
            writer.writeDouble("double_", double_);
            writer.writeBoolean("boolean_", boolean_);
            writer.writeChar("char_", char_);
            writer.writeString("string_", string_);
            writer.writeDecimal("bigDecimal_", bigDecimal_);
            writer.writeTime("localTime_", localTime_);
            writer.writeDate("localDate_", localDate_);
            writer.writeTimestamp("localDateTime_", localDateTime_);
            writer.writeTimestampWithTimezone("offsetDateTime_", offsetDateTime_);

            writer.writeByteArray("bytes", bytes);
            writer.writeShortArray("shorts", shorts);
            writer.writeIntArray("ints", ints);
            writer.writeLongArray("longs", longs);
            writer.writeFloatArray("floats", floats);
            writer.writeDoubleArray("doubles", doubles);
            writer.writeBooleanArray("booleans", booleans);
            writer.writeCharArray("chars", chars);
            writer.writeStringArray("strings", strings);
            writer.writeDecimalArray("bigDecimals", bigDecimals);
            writer.writeTimeArray("localTimes", localTimes);
            writer.writeDateArray("localDates", localDates);
            writer.writeTimestampArray("localDateTimes", localDateTimes);
            writer.writeTimestampWithTimezoneArray("offsetDateTimes", offsetDateTimes);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            byte_ = reader.readByte("byte_");
            short_ = reader.readShort("short_");
            int_ = reader.readInt("int_");
            long_ = reader.readLong("long_");
            float_ = reader.readFloat("float_");
            double_ = reader.readDouble("double_");
            boolean_ = reader.readBoolean("boolean_");
            char_ = reader.readChar("char_");
            string_ = reader.readString("string_");
            bigDecimal_ = reader.readDecimal("bigDecimal_");
            localTime_ = reader.readTime("localTime_");
            localDate_ = reader.readDate("localDate_");
            localDateTime_ = reader.readTimestamp("localDateTime_");
            offsetDateTime_ = reader.readTimestampWithTimezone("offsetDateTime_");

            bytes = reader.readByteArray("bytes");
            shorts = reader.readShortArray("shorts");
            ints = reader.readIntArray("ints");
            longs = reader.readLongArray("longs");
            floats = reader.readFloatArray("floats");
            doubles = reader.readDoubleArray("doubles");
            booleans = reader.readBooleanArray("booleans");
            chars = reader.readCharArray("chars");
            strings = reader.readStringArray("strings");
            bigDecimals = reader.readDecimalArray("bigDecimals");
            localTimes = reader.readTimeArray("localTimes");
            localDates = reader.readDateArray("localDates");
            localDateTimes = reader.readTimestampArray("localDateTimes");
            offsetDateTimes = reader.readTimestampWithTimezoneArray("offsetDateTimes");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PrimitivePortable that = (PrimitivePortable) o;
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
                case Char:
                    return char_;
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
                case Char:
                    return chars;
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
                case CharArray:
                    return chars;
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
                default:
                    throw new RuntimeException("Unsupported array method " + primitiveFields);
            }
        }
    }

    static class GroupPortable implements Portable {

        static final int FACTORY_ID = 1;
        static final int ID = 11;

        Portable portable;
        Portable[] portables;

        GroupPortable() {
        }

        GroupPortable(Portable portable) {
            this.portable = portable;
        }

        GroupPortable(Portable[] portables) {
            if (portables != null && portables.length > 0) {
                this.portable = portables[0];
            }
            this.portables = portables;
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writePortable("portable", portable);
            writer.writePortableArray("portables", portables);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            portable = reader.readPortable("portable");
            portables = reader.readPortableArray("portables");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupPortable that = (GroupPortable) o;
            return Arrays.equals(portables, that.portables);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(portables);
        }

        @Override
        public String toString() {
            String result = "";
            if (portable != null) {
                result += "portable=" + portable.toString() + ", ";
            } else {
                result += "portable=null, ";
            }
            if (portables != null) {
                result += "portables=";
                for (Portable portable : portables) {
                    result += portable.toString() + ", ";
                }
            }
            if (portable == null && portables == null) {
                result += "portables=null";
            }
            if (portable == null && portables != null && portables.length == 0) {
                result += "portables.length==0";
            }
            return "GroupPortable{" + result + "}";
        }
    }

    static class NestedGroupPortable implements Portable {

        static final int FACTORY_ID = 1;
        static final int ID = 12;

        Portable portable;
        Portable[] portables;

        NestedGroupPortable() {
        }

        NestedGroupPortable(Portable portable) {
            this.portable = portable;
            this.portables = new Portable[]{portable};
        }

        NestedGroupPortable(Portable[] portables) {
            if (portables != null && portables.length > 0) {
                this.portable = portables[0];
            }
            this.portables = portables;
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writePortable("portable", portable);
            writer.writePortableArray("portables", portables);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            portable = reader.readPortable("portable");
            portables = reader.readPortableArray("portables");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupPortable that = (GroupPortable) o;
            return Arrays.equals(portables, that.portables);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(portables);
        }

        @Override
        public String toString() {
            String result = "";
            if (portable != null) {
                result += "portable=" + portable.toString() + ", ";
            } else {
                result += "portable=null, ";
            }
            if (portables != null) {
                result += "portables=";
                for (Portable portable : portables) {
                    result += portable.toString() + ", ";
                }
            }
            if (portable == null && portables == null) {
                result += "portables=null";
            }
            if (portable == null && portables != null && portables.length == 0) {
                result += "portables.length==0";
            }
            return "NestedGroupPortable{" + result + "}";
        }
    }

    static class TestPortableFactory implements PortableFactory {
        static final int ID = 1;

        @Override
        public Portable create(int classId) {
            if (PrimitivePortable.ID == classId) {
                return new PrimitivePortable();
            } else if (GroupPortable.ID == classId) {
                return new GroupPortable();
            } else if (NestedGroupPortable.ID == classId) {
                return new NestedGroupPortable();
            }
            return null;
        }
    }

    static GroupPortable group(PrimitivePortable.Init init) {
        PrimitivePortable[] portables;
        if (init == PrimitivePortable.Init.FULL) {
            int count = 3;
            portables = new PrimitivePortable[count];
            for (int i = 0; i < count; i++) {
                portables[i] = new PrimitivePortable(i, PrimitivePortable.Init.FULL);
            }
        } else if (init == PrimitivePortable.Init.NULL) {
            portables = null;
        } else {
            portables = new PrimitivePortable[0];
        }
        return new GroupPortable(portables);
    }

    static GroupPortable group(PrimitivePortable portable) {
        return new GroupPortable(portable);
    }

    static NestedGroupPortable nested(GroupPortable portable) {
        return new NestedGroupPortable(portable);
    }

    static NestedGroupPortable nested(Portable[] portables) {
        return new NestedGroupPortable(portables);
    }

    static GroupPortable group(PrimitivePortable... portables) {
        return new GroupPortable(portables);
    }

    static PrimitivePortable prim(PrimitivePortable.Init init) {
        return new PrimitivePortable(1, init);
    }

    static PrimitivePortable prim(int seed, PrimitivePortable.Init init) {
        return new PrimitivePortable(seed, init);
    }
}

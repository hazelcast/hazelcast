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

package com.hazelcast.internal.serialization.impl.portable.portablereader;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.PrimitivePortable.Init.FULL;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.PrimitivePortable.Init.NULL;
import static java.util.Arrays.asList;

public class DefaultPortableReaderTestStructure {

    public enum Method {
        Byte("byte_"),
        Boolean("boolean_"),
        Char("char_"),
        Short("short_"),
        Int("int_"),
        Long("long_"),
        Float("float_"),
        Double("double_"),
        UTF("string_"),

        ByteArray("bytes"),
        BooleanArray("booleans"),
        CharArray("chars"),
        ShortArray("shorts"),
        IntArray("ints"),
        LongArray("longs"),
        FloatArray("floats"),
        DoubleArray("doubles"),
        UTFArray("strings"),

        PortableArray("portables"),
        Portable("portable"),

        Generic("");

        Method(String field) {
            this.field = field;
        }

        final String field;

        static List<Method> getPrimitives(boolean withUTF) {
            if (withUTF) {
                return asList(Byte, Boolean, Char, Short, Int, Long, Float, Double, UTF);
            } else {
                return asList(Byte, Boolean, Char, Short, Int, Long, Float, Double);
            }
        }

        static List<Method> getPrimitiveArrays() {
            return asList(ByteArray, BooleanArray, CharArray, ShortArray, IntArray, LongArray, FloatArray, DoubleArray, UTFArray);
        }

        static Method getArrayMethodFor(Method method) {
            switch (method) {
                case Byte:
                    return ByteArray;
                case Boolean:
                    return BooleanArray;
                case Char:
                    return CharArray;
                case Short:
                    return ShortArray;
                case Int:
                    return IntArray;
                case Long:
                    return LongArray;
                case Float:
                    return FloatArray;
                case Double:
                    return DoubleArray;
                case UTF:
                    return UTFArray;
                default:
                    throw new RuntimeException("Unsupported method");
            }
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

        byte[] bytes;
        short[] shorts;
        int[] ints;
        long[] longs;
        float[] floats;
        double[] doubles;
        boolean[] booleans;
        char[] chars;
        String[] strings;

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

                string_ = "";
            } else {
                string_ = null;
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
            writer.writeUTF("string_", string_);

            writer.writeByteArray("bytes", bytes);
            writer.writeShortArray("shorts", shorts);
            writer.writeIntArray("ints", ints);
            writer.writeLongArray("longs", longs);
            writer.writeFloatArray("floats", floats);
            writer.writeDoubleArray("doubles", doubles);
            writer.writeBooleanArray("booleans", booleans);
            writer.writeCharArray("chars", chars);
            writer.writeUTFArray("strings", strings);
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
            string_ = reader.readUTF("string_");

            bytes = reader.readByteArray("bytes");
            shorts = reader.readShortArray("shorts");
            ints = reader.readIntArray("ints");
            longs = reader.readLongArray("longs");
            floats = reader.readFloatArray("floats");
            doubles = reader.readDoubleArray("doubles");
            booleans = reader.readBooleanArray("booleans");
            chars = reader.readCharArray("chars");
            strings = reader.readUTFArray("strings");
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

        Object getPrimitive(Method method) {
            switch (method) {
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
                default:
                    throw new RuntimeException("Unsupported method " + method);
            }
        }

        Object getPrimitiveArray(Method method) {
            switch (method) {
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
                default:
                    throw new RuntimeException("Unsupported array method " + method);
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
        if (init == FULL) {
            int count = 3;
            portables = new PrimitivePortable[count];
            for (int i = 0; i < count; i++) {
                portables[i] = new PrimitivePortable(i, FULL);
            }
        } else if (init == NULL) {
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

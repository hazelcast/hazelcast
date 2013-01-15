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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.nio.serialization.SerializationConstants.*;

/**
 * @mdogan 6/18/12
 */
public class ConstantSerializers {

    public static final class ByteSerializer extends SingletonTypeSerializer<Byte> {

        public int getTypeId() {
            return CONSTANT_TYPE_BYTE;
        }

        public Byte read(final ObjectDataInput in) throws IOException {
            return in.readByte();
        }

        public void write(final ObjectDataOutput out, final Byte obj) throws IOException {
            out.writeByte(obj);
        }
    }

    public static final class BooleanSerializer extends SingletonTypeSerializer<Boolean> {

        public int getTypeId() {
            return CONSTANT_TYPE_BOOLEAN;
        }

        public void write(ObjectDataOutput out, Boolean obj) throws IOException {
            out.write((obj ? 1 : 0));
        }

        public Boolean read(ObjectDataInput in) throws IOException {
            return in.readByte() != 0;
        }
    }

    public static final class CharSerializer extends SingletonTypeSerializer<Character> {

        public int getTypeId() {
            return CONSTANT_TYPE_CHAR;
        }

        public Character read(final ObjectDataInput in) throws IOException {
            return in.readChar();
        }

        public void write(final ObjectDataOutput out, final Character obj) throws IOException {
            out.writeChar(obj);
        }
    }

    public static final class ShortSerializer extends SingletonTypeSerializer<Short> {

        public int getTypeId() {
            return CONSTANT_TYPE_SHORT;
        }

        public Short read(final ObjectDataInput in) throws IOException {
            return in.readShort();
        }

        public void write(final ObjectDataOutput out, final Short obj) throws IOException {
            out.writeShort(obj);
        }
    }

    public static final class IntegerSerializer extends SingletonTypeSerializer<Integer> {

        public int getTypeId() {
            return CONSTANT_TYPE_INTEGER;
        }

        public Integer read(final ObjectDataInput in) throws IOException {
            return in.readInt();
        }

        public void write(final ObjectDataOutput out, final Integer obj) throws IOException {
            out.writeInt(obj);
        }
    }

    public static final class LongSerializer extends SingletonTypeSerializer<Long> {

        public int getTypeId() {
            return CONSTANT_TYPE_LONG;
        }

        public Long read(final ObjectDataInput in) throws IOException {
            return in.readLong();
        }

        public void write(final ObjectDataOutput out, final Long obj) throws IOException {
            out.writeLong(obj);
        }
    }

    public static final class FloatSerializer extends SingletonTypeSerializer<Float> {

        public int getTypeId() {
            return CONSTANT_TYPE_FLOAT;
        }

        public Float read(final ObjectDataInput in) throws IOException {
            return in.readFloat();
        }

        public void write(final ObjectDataOutput out, final Float obj) throws IOException {
            out.writeFloat(obj);
        }
    }

    public static final class DoubleSerializer extends SingletonTypeSerializer<Double> {

        public int getTypeId() {
            return CONSTANT_TYPE_DOUBLE;
        }

        public Double read(final ObjectDataInput in) throws IOException {
            return in.readDouble();
        }

        public void write(final ObjectDataOutput out, final Double obj) throws IOException {
            out.writeDouble(obj);
        }
    }

    public static final class StringSerializer extends SingletonTypeSerializer<String> {

        public int getTypeId() {
            return CONSTANT_TYPE_STRING;
        }

        public String read(final ObjectDataInput in) throws IOException {
            return in.readUTF();
        }

        public void write(final ObjectDataOutput out, final String obj) throws IOException {
            out.writeUTF(obj);
        }
    }

    public static final class ByteArraySerializer extends SingletonTypeSerializer<byte[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_BYTE_ARRAY;
        }

        public byte[] read(final ObjectDataInput in) throws IOException {
            return IOUtil.readByteArray(in);
        }

        public void write(final ObjectDataOutput out, final byte[] obj) throws IOException {
            IOUtil.writeByteArray(out, obj);
        }
    }

    public static final class CharArraySerializer extends SingletonTypeSerializer<char[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_CHAR_ARRAY;
        }

        public char[] read(final ObjectDataInput in) throws IOException {
            int size = in.readInt();
            if (size == 0) {
                return null;
            } else {
                char[] c = new char[size];
                for (int i = 0; i < size; i++) {
                    c[i] = in.readChar();
                }
                return c;
            }
        }

        public void write(final ObjectDataOutput out, final char[] obj) throws IOException {
            int size = (obj == null) ? 0 : obj.length;
            out.writeInt(size);
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    out.writeChar(obj[i]);
                }
            }
        }
    }

    public static final class ShortArraySerializer extends SingletonTypeSerializer<short[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_SHORT_ARRAY;
        }

        public short[] read(final ObjectDataInput in) throws IOException {
            int size = in.readInt();
            if (size == 0) {
                return null;
            } else {
                short[] s = new short[size];
                for (int i = 0; i < size; i++) {
                    s[i] = in.readShort();
                }
                return s;
            }
        }

        public void write(final ObjectDataOutput out, final short[] obj) throws IOException {
            int size = (obj == null) ? 0 : obj.length;
            out.writeInt(size);
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    out.writeShort(obj[i]);
                }
            }
        }
    }

    public static final class IntegerArraySerializer extends SingletonTypeSerializer<int[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_INTEGER_ARRAY;
        }

        public int[] read(final ObjectDataInput in) throws IOException {
            int size = in.readInt();
            if (size == 0) {
                return null;
            } else {
                int[] s = new int[size];
                for (int i = 0; i < size; i++) {
                    s[i] = in.readInt();
                }
                return s;
            }
        }

        public void write(final ObjectDataOutput out, final int[] obj) throws IOException {
            int size = (obj == null) ? 0 : obj.length;
            out.writeInt(size);
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    out.writeInt(obj[i]);
                }
            }
        }
    }

    public static final class LongArraySerializer extends SingletonTypeSerializer<long[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_LONG_ARRAY;
        }

        public long[] read(final ObjectDataInput in) throws IOException {
            int size = in.readInt();
            if (size == 0) {
                return null;
            } else {
                long[] l = new long[size];
                for (int i = 0; i < size; i++) {
                    l[i] = in.readLong();
                }
                return l;
            }
        }

        public void write(final ObjectDataOutput out, final long[] obj) throws IOException {
            int size = (obj == null) ? 0 : obj.length;
            out.writeInt(size);
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    out.writeLong(obj[i]);
                }
            }
        }
    }

    public static final class FloatArraySerializer extends SingletonTypeSerializer<float[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_FLOAT_ARRAY;
        }

        public float[] read(final ObjectDataInput in) throws IOException {
            int size = in.readInt();
            if (size == 0) {
                return null;
            } else {
                float [] f = new float[size];
                for (int i = 0; i < size; i++) {
                    f[i] = in.readFloat();
                }
                return f;
            }
        }

        public void write(final ObjectDataOutput out, final float[] obj) throws IOException {
            int size = (obj == null) ? 0 : obj.length;
            out.writeInt(size);
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    out.writeFloat(obj[i]);
                }
            }
        }
    }

    public static final class DoubleArraySerializer extends SingletonTypeSerializer<double[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_DOUBLE_ARRAY;
        }

        public double[] read(final ObjectDataInput in) throws IOException {
            int size = in.readInt();
            if (size == 0) {
                return null;
            } else {
                double [] d = new double[size];
                for (int i = 0; i < size; i++) {
                    d[i] = in.readDouble();
                }
                return d;
            }
        }

        public void write(final ObjectDataOutput out, final double [] obj) throws IOException {
            int size = (obj == null) ? 0 : obj.length;
            out.writeInt(size);
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    out.writeDouble(obj[i]);
                }
            }
        }
    }

    private abstract static class SingletonTypeSerializer<T> implements TypeSerializer<T> {

        public void destroy() {
        }
    }

    private ConstantSerializers() {}

}

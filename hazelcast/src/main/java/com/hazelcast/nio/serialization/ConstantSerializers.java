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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_CHAR;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_SHORT;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_FLOAT;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_DOUBLE;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_INTEGER;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_BOOLEAN;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_BYTE;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_LONG;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_STRING;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_CHAR_ARRAY;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_SHORT_ARRAY;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_LONG_ARRAY;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_INTEGER_ARRAY;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_FLOAT_ARRAY;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_DOUBLE_ARRAY;


public final class ConstantSerializers {

    public static final class ByteSerializer extends SingletonSerializer<Byte> {

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

    public static final class BooleanSerializer extends SingletonSerializer<Boolean> {

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

    public static final class CharSerializer extends SingletonSerializer<Character> {

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

    public static final class ShortSerializer extends SingletonSerializer<Short> {

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

    public static final class IntegerSerializer extends SingletonSerializer<Integer> {

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

    public static final class LongSerializer extends SingletonSerializer<Long> {

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

    public static final class FloatSerializer extends SingletonSerializer<Float> {

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

    public static final class DoubleSerializer extends SingletonSerializer<Double> {

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

    public static final class StringSerializer extends SingletonSerializer<String> {

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

    public static final class TheByteArraySerializer implements ByteArraySerializer<byte[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_BYTE_ARRAY;
        }

        @Override
        public byte[] write(byte[] object) throws IOException {
            return object;
        }

        @Override
        public byte[] read(byte[] buffer) throws IOException {
            return buffer;
        }

        public void destroy() {
        }
    }

    public static final class CharArraySerializer extends SingletonSerializer<char[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_CHAR_ARRAY;
        }

        public char[] read(final ObjectDataInput in) throws IOException {
            return in.readCharArray();
        }

        public void write(final ObjectDataOutput out, final char[] obj) throws IOException {
            out.writeCharArray(obj);
        }
    }

    public static final class ShortArraySerializer extends SingletonSerializer<short[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_SHORT_ARRAY;
        }

        public short[] read(final ObjectDataInput in) throws IOException {
            return in.readShortArray();
        }

        public void write(final ObjectDataOutput out, final short[] obj) throws IOException {
            out.writeShortArray(obj);
        }
    }

    public static final class IntegerArraySerializer extends SingletonSerializer<int[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_INTEGER_ARRAY;
        }

        public int[] read(final ObjectDataInput in) throws IOException {
            return in.readIntArray();
        }

        public void write(final ObjectDataOutput out, final int[] obj) throws IOException {
            out.writeIntArray(obj);
        }
    }

    public static final class LongArraySerializer extends SingletonSerializer<long[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_LONG_ARRAY;
        }

        public long[] read(final ObjectDataInput in) throws IOException {
            return in.readLongArray();
        }

        public void write(final ObjectDataOutput out, final long[] obj) throws IOException {
            out.writeLongArray(obj);
        }
    }

    public static final class FloatArraySerializer extends SingletonSerializer<float[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_FLOAT_ARRAY;
        }

        public float[] read(final ObjectDataInput in) throws IOException {
            return in.readFloatArray();
        }

        public void write(final ObjectDataOutput out, final float[] obj) throws IOException {
            out.writeFloatArray(obj);
        }
    }

    public static final class DoubleArraySerializer extends SingletonSerializer<double[]> {

        public int getTypeId() {
            return CONSTANT_TYPE_DOUBLE_ARRAY;
        }

        public double[] read(final ObjectDataInput in) throws IOException {
            return in.readDoubleArray();
        }

        public void write(final ObjectDataOutput out, final double[] obj) throws IOException {
            out.writeDoubleArray(obj);
        }
    }

    private abstract static class SingletonSerializer<T> implements StreamSerializer<T> {

        public void destroy() {
        }
    }

    private ConstantSerializers() {
    }

}

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

package com.hazelcast.internal.serialization.impl.defaultserializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.nio.serialization.StreamSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.UUID;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_BOOLEAN;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_BOOLEAN_ARRAY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_BYTE;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_CHAR;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_CHAR_ARRAY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DOUBLE;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DOUBLE_ARRAY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_FLOAT;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_FLOAT_ARRAY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_INTEGER;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_INTEGER_ARRAY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_LONG;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_LONG_ARRAY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_NULL;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_SHORT;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_SHORT_ARRAY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_SIMPLE_ENTRY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_SIMPLE_IMMUTABLE_ENTRY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_STRING;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_STRING_ARRAY;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_UUID;

public final class ConstantSerializers {

    public static final class NullSerializer extends SingletonSerializer<Object> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_NULL;
        }

        @SuppressFBWarnings(
                value = "NP_NONNULL_RETURN_VIOLATION",
                justification = "NullSerializer is only used for null values.")
        @Override
        public Object read(final ObjectDataInput in) throws IOException {
            return null;
        }

        @Override
        public void write(final ObjectDataOutput out, final Object obj) throws IOException {
        }
    }

    public static final class ByteSerializer extends SingletonSerializer<Byte> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_BYTE;
        }

        @Override
        public Byte read(final ObjectDataInput in) throws IOException {
            return in.readByte();
        }

        @Override
        public void write(final ObjectDataOutput out, final Byte obj) throws IOException {
            out.writeByte(obj);
        }
    }

    public static final class BooleanSerializer extends SingletonSerializer<Boolean> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_BOOLEAN;
        }

        @Override
        public void write(ObjectDataOutput out, Boolean obj) throws IOException {
            out.write((obj ? 1 : 0));
        }

        @Override
        public Boolean read(ObjectDataInput in) throws IOException {
            return in.readByte() != 0;
        }
    }

    public static final class CharSerializer extends SingletonSerializer<Character> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_CHAR;
        }

        @Override
        public Character read(final ObjectDataInput in) throws IOException {
            return in.readChar();
        }

        @Override
        public void write(final ObjectDataOutput out, final Character obj) throws IOException {
            out.writeChar(obj);
        }
    }

    public static final class ShortSerializer extends SingletonSerializer<Short> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_SHORT;
        }

        @Override
        public Short read(final ObjectDataInput in) throws IOException {
            return in.readShort();
        }

        @Override
        public void write(final ObjectDataOutput out, final Short obj) throws IOException {
            out.writeShort(obj);
        }
    }

    public static final class IntegerSerializer extends SingletonSerializer<Integer> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_INTEGER;
        }

        @Override
        public Integer read(final ObjectDataInput in) throws IOException {
            return in.readInt();
        }

        @Override
        public void write(final ObjectDataOutput out, final Integer obj) throws IOException {
            out.writeInt(obj);
        }
    }

    public static final class LongSerializer extends SingletonSerializer<Long> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_LONG;
        }

        @Override
        public Long read(final ObjectDataInput in) throws IOException {
            return in.readLong();
        }

        @Override
        public void write(final ObjectDataOutput out, final Long obj) throws IOException {
            out.writeLong(obj);
        }
    }

    public static final class FloatSerializer extends SingletonSerializer<Float> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_FLOAT;
        }

        @Override
        public Float read(final ObjectDataInput in) throws IOException {
            return in.readFloat();
        }

        @Override
        public void write(final ObjectDataOutput out, final Float obj) throws IOException {
            out.writeFloat(obj);
        }
    }

    public static final class DoubleSerializer extends SingletonSerializer<Double> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_DOUBLE;
        }

        @Override
        public Double read(final ObjectDataInput in) throws IOException {
            return in.readDouble();
        }

        @Override
        public void write(final ObjectDataOutput out, final Double obj) throws IOException {
            out.writeDouble(obj);
        }
    }

    public static final class StringSerializer extends SingletonSerializer<String> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_STRING;
        }

        @Override
        public String read(final ObjectDataInput in) throws IOException {
            return in.readString();
        }

        @Override
        public void write(final ObjectDataOutput out, final String obj) throws IOException {
            out.writeString(obj);
        }
    }

    public static final class TheByteArraySerializer implements ByteArraySerializer<byte[]> {

        @Override
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

        @Override
        public void destroy() {
        }
    }

    public static final class BooleanArraySerializer extends SingletonSerializer<boolean[]> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_BOOLEAN_ARRAY;
        }

        @Override
        public boolean[] read(final ObjectDataInput in) throws IOException {
            return in.readBooleanArray();
        }

        @Override
        public void write(final ObjectDataOutput out, final boolean[] obj) throws IOException {
            out.writeBooleanArray(obj);
        }
    }

    public static final class CharArraySerializer extends SingletonSerializer<char[]> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_CHAR_ARRAY;
        }

        @Override
        public char[] read(final ObjectDataInput in) throws IOException {
            return in.readCharArray();
        }

        @Override
        public void write(final ObjectDataOutput out, final char[] obj) throws IOException {
            out.writeCharArray(obj);
        }
    }

    public static final class ShortArraySerializer extends SingletonSerializer<short[]> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_SHORT_ARRAY;
        }

        @Override
        public short[] read(final ObjectDataInput in) throws IOException {
            return in.readShortArray();
        }

        @Override
        public void write(final ObjectDataOutput out, final short[] obj) throws IOException {
            out.writeShortArray(obj);
        }
    }

    public static final class IntegerArraySerializer extends SingletonSerializer<int[]> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_INTEGER_ARRAY;
        }

        @Override
        public int[] read(final ObjectDataInput in) throws IOException {
            return in.readIntArray();
        }

        @Override
        public void write(final ObjectDataOutput out, final int[] obj) throws IOException {
            out.writeIntArray(obj);
        }
    }

    public static final class LongArraySerializer extends SingletonSerializer<long[]> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_LONG_ARRAY;
        }

        @Override
        public long[] read(final ObjectDataInput in) throws IOException {
            return in.readLongArray();
        }

        @Override
        public void write(final ObjectDataOutput out, final long[] obj) throws IOException {
            out.writeLongArray(obj);
        }
    }

    public static final class FloatArraySerializer extends SingletonSerializer<float[]> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_FLOAT_ARRAY;
        }

        @Override
        public float[] read(final ObjectDataInput in) throws IOException {
            return in.readFloatArray();
        }

        @Override
        public void write(final ObjectDataOutput out, final float[] obj) throws IOException {
            out.writeFloatArray(obj);
        }
    }

    public static final class DoubleArraySerializer extends SingletonSerializer<double[]> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_DOUBLE_ARRAY;
        }

        @Override
        public double[] read(final ObjectDataInput in) throws IOException {
            return in.readDoubleArray();
        }

        @Override
        public void write(final ObjectDataOutput out, final double[] obj) throws IOException {
            out.writeDoubleArray(obj);
        }
    }

    public static final class StringArraySerializer extends SingletonSerializer<String[]> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_STRING_ARRAY;
        }

        @Override
        public String[] read(final ObjectDataInput in) throws IOException {
            return in.readStringArray();
        }

        @Override
        public void write(final ObjectDataOutput out, final String[] obj) throws IOException {
            out.writeStringArray(obj);
        }
    }

    public static final class UuidSerializer extends SingletonSerializer<UUID> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_UUID;
        }

        @Override
        public UUID read(final ObjectDataInput in) throws IOException {
            return new UUID(in.readLong(), in.readLong());
        }

        @Override
        public void write(final ObjectDataOutput out, final UUID uuid) throws IOException {
            out.writeLong(uuid.getMostSignificantBits());
            out.writeLong(uuid.getLeastSignificantBits());
        }
    }

    public static final class SimpleEntrySerializer extends SingletonSerializer<AbstractMap.SimpleEntry> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_SIMPLE_ENTRY;
        }

        @Override
        public AbstractMap.SimpleEntry read(final ObjectDataInput in) throws IOException {
            return new AbstractMap.SimpleEntry(in.readObject(), in.readObject());
        }

        @Override
        public void write(final ObjectDataOutput out, final AbstractMap.SimpleEntry entry) throws IOException {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    public static final class SimpleImmutableEntrySerializer extends SingletonSerializer<AbstractMap.SimpleImmutableEntry> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_SIMPLE_IMMUTABLE_ENTRY;
        }

        @Override
        public AbstractMap.SimpleImmutableEntry read(final ObjectDataInput in) throws IOException {
            return new AbstractMap.SimpleImmutableEntry(in.readObject(), in.readObject());
        }

        @Override
        public void write(final ObjectDataOutput out, final AbstractMap.SimpleImmutableEntry entry) throws IOException {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    private abstract static class SingletonSerializer<T> implements StreamSerializer<T> {

        @Override
        public void destroy() {
        }
    }

    private ConstantSerializers() {
    }
}

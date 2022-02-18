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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.Arrays;

/**
 * Sample DataSerializable for testing internal constant serializers
 */
public class SerializationV1DataSerializable implements DataSerializable {

    byte aByte;
    boolean aBoolean;
    char character;
    short aShort;
    int integer;
    long aLong;
    float aFloat;
    double aDouble;
    byte[] bytes;
    boolean[] booleans;
    char[] chars;
    short[] shorts;
    int[] ints;
    long[] longs;
    float[] floats;
    double[] doubles;
    String string;
    String[] strings;

    // used to assert version provided in ObjectDataInput & ObjectDataOutput in read/writeData methods
    Version version;

    public SerializationV1DataSerializable() {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public SerializationV1DataSerializable(byte aByte, boolean aBoolean, char character, short aShort, int integer, long aLong,
                                           float aFloat, double aDouble, byte[] bytes, boolean[] booleans, char[] chars,
                                           short[] shorts, int[] ints, long[] longs, float[] floats, double[] doubles,
                                           String string, String[] strings) {
        this.aByte = aByte;
        this.aBoolean = aBoolean;
        this.character = character;
        this.aShort = aShort;
        this.integer = integer;
        this.aLong = aLong;
        this.aFloat = aFloat;
        this.aDouble = aDouble;
        this.bytes = bytes;
        this.booleans = booleans;
        this.chars = chars;
        this.shorts = shorts;
        this.ints = ints;
        this.longs = longs;
        this.floats = floats;
        this.doubles = doubles;
        this.string = string;
        this.strings = strings;
    }

    public Version getVersion() {
        return version;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(aByte);
        out.writeBoolean(aBoolean);
        out.writeChar(character);
        out.writeShort(aShort);
        out.writeInt(integer);
        out.writeLong(aLong);
        out.writeFloat(aFloat);
        out.writeDouble(aDouble);

        out.writeByteArray(bytes);
        out.writeBooleanArray(booleans);
        out.writeCharArray(chars);
        out.writeShortArray(shorts);
        out.writeIntArray(ints);
        out.writeLongArray(longs);
        out.writeFloatArray(floats);
        out.writeDoubleArray(doubles);
        out.writeString(string);
        out.writeUTFArray(strings);

        this.version = out.getVersion();
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.aByte = in.readByte();
        this.aBoolean = in.readBoolean();
        this.character = in.readChar();
        this.aShort = in.readShort();
        this.integer = in.readInt();
        this.aLong = in.readLong();
        this.aFloat = in.readFloat();
        this.aDouble = in.readDouble();
        this.bytes = in.readByteArray();
        this.booleans = in.readBooleanArray();
        this.chars = in.readCharArray();
        this.shorts = in.readShortArray();
        this.ints = in.readIntArray();
        this.longs = in.readLongArray();
        this.floats = in.readFloatArray();
        this.doubles = in.readDoubleArray();
        this.string = in.readString();
        this.strings = in.readStringArray();

        this.version = in.getVersion();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SerializationV1DataSerializable that = (SerializationV1DataSerializable) o;
        if (aByte != that.aByte) {
            return false;
        }
        if (aBoolean != that.aBoolean) {
            return false;
        }
        if (character != that.character) {
            return false;
        }
        if (aShort != that.aShort) {
            return false;
        }
        if (integer != that.integer) {
            return false;
        }
        if (aLong != that.aLong) {
            return false;
        }
        if (Float.compare(that.aFloat, aFloat) != 0) {
            return false;
        }
        if (Double.compare(that.aDouble, aDouble) != 0) {
            return false;
        }
        if (!Arrays.equals(bytes, that.bytes)) {
            return false;
        }
        if (!Arrays.equals(booleans, that.booleans)) {
            return false;
        }
        if (!Arrays.equals(chars, that.chars)) {
            return false;
        }
        if (!Arrays.equals(shorts, that.shorts)) {
            return false;
        }
        if (!Arrays.equals(ints, that.ints)) {
            return false;
        }
        if (!Arrays.equals(longs, that.longs)) {
            return false;
        }
        if (!Arrays.equals(floats, that.floats)) {
            return false;
        }
        if (!Arrays.equals(doubles, that.doubles)) {
            return false;
        }
        if (string != null ? !string.equals(that.string) : that.string != null) {
            return false;
        }
        if (!Arrays.equals(strings, that.strings)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (int) aByte;
        result = 31 * result + (aBoolean ? 1 : 0);
        result = 31 * result + (int) character;
        result = 31 * result + (int) aShort;
        result = 31 * result + integer;
        result = 31 * result + (int) (aLong ^ (aLong >>> 32));
        result = 31 * result + (aFloat != +0.0f ? Float.floatToIntBits(aFloat) : 0);
        temp = Double.doubleToLongBits(aDouble);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + Arrays.hashCode(bytes);
        result = 31 * result + Arrays.hashCode(booleans);
        result = 31 * result + Arrays.hashCode(chars);
        result = 31 * result + Arrays.hashCode(shorts);
        result = 31 * result + Arrays.hashCode(ints);
        result = 31 * result + Arrays.hashCode(longs);
        result = 31 * result + Arrays.hashCode(floats);
        result = 31 * result + Arrays.hashCode(doubles);
        result = 31 * result + (string != null ? string.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(strings);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }

    public static SerializationV1DataSerializable createInstanceWithNonNullFields() {
        return new SerializationV1DataSerializable((byte) 99, true, 'c', (short) 11, 1234134, 1341431221L, 1.12312f, 432.424,
                new byte[]{(byte) 1, (byte) 2, (byte) 3}, new boolean[]{true, false, true}, new char[]{'a', 'b', 'c'},
                new short[]{1, 2, 3}, new int[]{4, 2, 3}, new long[]{11, 2, 3}, new float[]{1.0f, 2.1f, 3.4f},
                new double[]{11.1, 22.2, 33.3}, "the string text", new String[]{"item1", "item2", "item3"});
    }
}

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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.impl.SerializationV1DataSerializable;
import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.Arrays;

/**
 * Sample DataSerializable for testing internal constant serializers
 */
public class SerializationV1Portable implements Portable {

    static final NamedPortable INNER_PORTABLE = new NamedPortable("name", 1);

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
    NamedPortable innerPortable;
    DataSerializable dataSerializable;

    public SerializationV1Portable() {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public SerializationV1Portable(byte aByte, boolean aBoolean, char character, short aShort, int integer, long aLong,
                                   float aFloat, double aDouble, byte[] bytes, boolean[] booleans, char[] chars, short[] shorts,
                                   int[] ints, long[] longs, float[] floats, double[] doubles, String string, String[] strings,
                                   NamedPortable innerPortable, DataSerializable dataSerializable) {
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
        this.innerPortable = innerPortable;
        this.dataSerializable = dataSerializable;
    }

    @Override
    public int getFactoryId() {
        return TestSerializationConstants.PORTABLE_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TestSerializationConstants.ALL_FIELD_OBJECT_PORTABLE;
    }

    @Override
    public void writePortable(PortableWriter out) throws IOException {
        out.writeByte("1", aByte);
        out.writeBoolean("2", aBoolean);
        out.writeChar("3", character);
        out.writeShort("4", aShort);
        out.writeInt("5", integer);
        out.writeLong("6", aLong);
        out.writeFloat("7", aFloat);
        out.writeDouble("8", aDouble);
        out.writeString("9", string);

        out.writeByteArray("a1", bytes);
        out.writeBooleanArray("a2", booleans);
        out.writeCharArray("a3", chars);
        out.writeShortArray("a4", shorts);
        out.writeIntArray("a5", ints);
        out.writeLongArray("a6", longs);
        out.writeFloatArray("a7", floats);
        out.writeDoubleArray("a8", doubles);
        out.writeStringArray("a9", strings);

        if (innerPortable == null) {
            out.writeNullPortable("p", INNER_PORTABLE.getFactoryId(), INNER_PORTABLE.getClassId());
        } else {
            out.writePortable("p", innerPortable);
        }

        ObjectDataOutput rawDataOutput = out.getRawDataOutput();
        boolean isNotNull = dataSerializable != null;
        if (isNotNull) {
            rawDataOutput.writeBoolean(isNotNull);
            dataSerializable.writeData(rawDataOutput);
        } else {
            rawDataOutput.writeBoolean(isNotNull);
        }
    }

    @Override
    public void readPortable(PortableReader in) throws IOException {
        this.aByte = in.readByte("1");
        this.aBoolean = in.readBoolean("2");
        this.character = in.readChar("3");
        this.aShort = in.readShort("4");
        this.integer = in.readInt("5");
        this.aLong = in.readLong("6");
        this.aFloat = in.readFloat("7");
        this.aDouble = in.readDouble("8");
        this.string = in.readString("9");

        this.bytes = in.readByteArray("a1");
        this.booleans = in.readBooleanArray("a2");
        this.chars = in.readCharArray("a3");
        this.shorts = in.readShortArray("a4");
        this.ints = in.readIntArray("a5");
        this.longs = in.readLongArray("a6");
        this.floats = in.readFloatArray("a7");
        this.doubles = in.readDoubleArray("a8");
        this.strings = in.readStringArray("a9");

        this.innerPortable = in.readPortable("p");

        ObjectDataInput rawDataInput = in.getRawDataInput();
        boolean isNotNull = rawDataInput.readBoolean();
        if (isNotNull) {
            SerializationV1DataSerializable dataserializable = new SerializationV1DataSerializable();
            dataserializable.readData(rawDataInput);
            this.dataSerializable = dataserializable;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SerializationV1Portable that = (SerializationV1Portable) o;
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
        if (innerPortable != null ? !innerPortable.equals(that.innerPortable) : that.innerPortable != null) {
            return false;
        }
        if (dataSerializable != null ? !dataSerializable.equals(that.dataSerializable) : that.dataSerializable != null) {
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
        result = 31 * result + (innerPortable != null ? innerPortable.hashCode() : 0);
        result = 31 * result + (dataSerializable != null ? dataSerializable.hashCode() : 0);
        return result;
    }

    public static SerializationV1Portable createInstanceWithNonNullFields() {
        SerializationV1DataSerializable dataserializable = SerializationV1DataSerializable.createInstanceWithNonNullFields();
        return new SerializationV1Portable((byte) 99, true, 'c', (short) 11, 1234134, 1341431221L, 1.12312f, 432.424,
                new byte[]{(byte) 1, (byte) 2, (byte) 3}, new boolean[]{true, false, true}, new char[]{'a', 'b', 'c'},
                new short[]{1, 2, 3}, new int[]{4, 2, 3}, new long[]{11, 2, 3}, new float[]{1.0f, 2.1f, 3.4f},
                new double[]{11.1, 22.2, 33.3}, "the string text", new String[]{"item1", "item2", "item3"}, INNER_PORTABLE,
                dataserializable);
    }
}

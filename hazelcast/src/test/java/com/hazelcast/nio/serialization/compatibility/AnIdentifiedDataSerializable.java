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

package com.hazelcast.nio.serialization.compatibility;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;

import java.io.IOException;
import java.util.Arrays;

public class AnIdentifiedDataSerializable implements IdentifiedDataSerializable {

    private boolean bool;
    private byte b;
    private char c;
    private double d;
    private short s;
    private float f;
    private int i;
    private long l;
    private String str;

    private boolean[] booleans;
    private byte[] bytes;
    private char[] chars;
    private double[] doubles;
    private short[] shorts;
    private float[] floats;
    private int[] ints;
    private long[] longs;
    private String[] strings;

    private boolean[] booleansNull;
    private byte[] bytesNull;
    private char[] charsNull;
    private double[] doublesNull;
    private short[] shortsNull;
    private float[] floatsNull;
    private int[] intsNull;
    private long[] longsNull;
    private String[] stringsNull;

    private byte byteSize;
    private byte[] bytesFully;
    private byte[] bytesOffset;
    private char[] strChars;
    private byte[] strBytes;
    private int unsignedByte;
    private int unsignedShort;

    private Object portableObject;
    private Object identifiedDataSerializableObject;
    private Object customStreamSerializableObject;
    private Object customByteArraySerializableObject;

    private Data data;

    @SuppressWarnings({"checkstyle:parameternumber", "checkstyle:executablestatementcount"})
    public AnIdentifiedDataSerializable(boolean bool, byte b, char c, double d, short s,
                                        float f, int i, long l, String str,
                                        boolean[] booleans, byte[] bytes, char[] chars, double[] doubles, short[] shorts,
                                        float[] floats, int[] ints, long[] longs, String[] strings,
                                        Portable portable,
                                        IdentifiedDataSerializable identifiedDataSerializable,
                                        CustomStreamSerializable customStreamSerializableObject,
                                        CustomByteArraySerializable customByteArraySerializableObject, Data data) {
        this.bool = bool;
        this.b = b;
        this.c = c;
        this.d = d;
        this.s = s;
        this.f = f;
        this.i = i;
        this.l = l;
        this.str = str;

        this.booleans = booleans;
        this.bytes = bytes;
        this.chars = chars;
        this.doubles = doubles;
        this.shorts = shorts;
        this.floats = floats;
        this.ints = ints;
        this.longs = longs;
        this.strings = strings;

        this.byteSize = (byte) bytes.length;
        this.bytesFully = bytes;
        this.bytesOffset = Arrays.copyOfRange(bytes, 1, 3);
        this.strChars = str.toCharArray();
        this.strBytes = new byte[str.length()];
        for (int j = 0; j < str.length(); j++) {
            strBytes[j] = (byte) strChars[j];
        }
        unsignedByte = Byte.MAX_VALUE + 100;
        unsignedShort = Short.MAX_VALUE + 100;

        this.identifiedDataSerializableObject = identifiedDataSerializable;
        this.portableObject = portable;
        this.customStreamSerializableObject = customStreamSerializableObject;
        this.customByteArraySerializableObject = customByteArraySerializableObject;

        this.data = data;
    }

    public AnIdentifiedDataSerializable() {
    }

    @Override
    public int getFactoryId() {
        return ReferenceObjects.IDENTIFIED_DATA_SERIALIZABLE_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ReferenceObjects.DATA_SERIALIZABLE_CLASS_ID;
    }

    @Override
    public void writeData(ObjectDataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(bool);
        dataOutput.writeByte(b);
        dataOutput.writeChar(c);
        dataOutput.writeDouble(d);
        dataOutput.writeShort(s);
        dataOutput.writeFloat(f);
        dataOutput.writeInt(i);
        dataOutput.writeLong(l);
        dataOutput.writeString(str);

        dataOutput.writeBooleanArray(booleans);
        dataOutput.writeByteArray(bytes);
        dataOutput.writeCharArray(chars);
        dataOutput.writeDoubleArray(doubles);
        dataOutput.writeShortArray(shorts);
        dataOutput.writeFloatArray(floats);
        dataOutput.writeIntArray(ints);
        dataOutput.writeLongArray(longs);
        dataOutput.writeUTFArray(strings);

        dataOutput.writeBooleanArray(booleansNull);
        dataOutput.writeByteArray(bytesNull);
        dataOutput.writeCharArray(charsNull);
        dataOutput.writeDoubleArray(doublesNull);
        dataOutput.writeShortArray(shortsNull);
        dataOutput.writeFloatArray(floatsNull);
        dataOutput.writeIntArray(intsNull);
        dataOutput.writeLongArray(longsNull);
        dataOutput.writeUTFArray(stringsNull);

        byteSize = (byte) bytes.length;
        dataOutput.write(byteSize);
        dataOutput.write(bytes);
        dataOutput.write(bytes, 1, 2);
        dataOutput.writeInt(str.length());
        dataOutput.writeChars(str);
        dataOutput.writeBytes(str);
        dataOutput.writeByte(unsignedByte);
        dataOutput.writeShort(unsignedShort);

        dataOutput.writeObject(portableObject);
        dataOutput.writeObject(identifiedDataSerializableObject);
        dataOutput.writeObject(customByteArraySerializableObject);
        dataOutput.writeObject(customStreamSerializableObject);

        IOUtil.writeData(dataOutput, data);
    }

    @Override
    public void readData(ObjectDataInput dataInput) throws IOException {
        bool = dataInput.readBoolean();
        b = dataInput.readByte();
        c = dataInput.readChar();
        d = dataInput.readDouble();
        s = dataInput.readShort();
        f = dataInput.readFloat();
        i = dataInput.readInt();
        l = dataInput.readLong();
        str = dataInput.readString();

        booleans = dataInput.readBooleanArray();
        bytes = dataInput.readByteArray();
        chars = dataInput.readCharArray();
        doubles = dataInput.readDoubleArray();
        shorts = dataInput.readShortArray();
        floats = dataInput.readFloatArray();
        ints = dataInput.readIntArray();
        longs = dataInput.readLongArray();
        strings = dataInput.readStringArray();

        booleansNull = dataInput.readBooleanArray();
        bytesNull = dataInput.readByteArray();
        charsNull = dataInput.readCharArray();
        doublesNull = dataInput.readDoubleArray();
        shortsNull = dataInput.readShortArray();
        floatsNull = dataInput.readFloatArray();
        intsNull = dataInput.readIntArray();
        longsNull = dataInput.readLongArray();
        stringsNull = dataInput.readStringArray();

        byteSize = dataInput.readByte();
        bytesFully = new byte[byteSize];
        dataInput.readFully(bytesFully);
        bytesOffset = new byte[2];
        dataInput.readFully(bytesOffset, 0, 2);
        int strSize = dataInput.readInt();
        strChars = new char[strSize];
        for (int j = 0; j < strSize; j++) {
            strChars[j] = dataInput.readChar();
        }
        strBytes = new byte[strSize];
        dataInput.readFully(strBytes);
        unsignedByte = dataInput.readUnsignedByte();
        unsignedShort = dataInput.readUnsignedShort();

        portableObject = dataInput.readObject();
        identifiedDataSerializableObject = dataInput.readObject();
        customByteArraySerializableObject = dataInput.readObject();
        customStreamSerializableObject = dataInput.readObject();

        data = IOUtil.readData(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AnIdentifiedDataSerializable that = (AnIdentifiedDataSerializable) o;
        if (bool != that.bool) {
            return false;
        }
        if (b != that.b) {
            return false;
        }
        if (c != that.c) {
            return false;
        }
        if (Double.compare(that.d, d) != 0) {
            return false;
        }
        if (s != that.s) {
            return false;
        }
        if (Float.compare(that.f, f) != 0) {
            return false;
        }
        if (i != that.i) {
            return false;
        }
        if (l != that.l) {
            return false;
        }
        if (byteSize != that.byteSize) {
            return false;
        }
        if (unsignedByte != that.unsignedByte) {
            return false;
        }
        if (unsignedShort != that.unsignedShort) {
            return false;
        }
        if (str != null ? !str.equals(that.str) : that.str != null) {
            return false;
        }
        if (!Arrays.equals(booleans, that.booleans)) {
            return false;
        }
        if (!Arrays.equals(bytes, that.bytes)) {
            return false;
        }
        if (!Arrays.equals(chars, that.chars)) {
            return false;
        }
        if (!Arrays.equals(doubles, that.doubles)) {
            return false;
        }
        if (!Arrays.equals(shorts, that.shorts)) {
            return false;
        }
        if (!Arrays.equals(floats, that.floats)) {
            return false;
        }
        if (!Arrays.equals(ints, that.ints)) {
            return false;
        }
        if (!Arrays.equals(longs, that.longs)) {
            return false;
        }
        if (!Arrays.equals(strings, that.strings)) {
            return false;
        }
        if (!Arrays.equals(booleansNull, that.booleansNull)) {
            return false;
        }
        if (!Arrays.equals(bytesNull, that.bytesNull)) {
            return false;
        }
        if (!Arrays.equals(charsNull, that.charsNull)) {
            return false;
        }
        if (!Arrays.equals(doublesNull, that.doublesNull)) {
            return false;
        }
        if (!Arrays.equals(shortsNull, that.shortsNull)) {
            return false;
        }
        if (!Arrays.equals(floatsNull, that.floatsNull)) {
            return false;
        }
        if (!Arrays.equals(intsNull, that.intsNull)) {
            return false;
        }
        if (!Arrays.equals(longsNull, that.longsNull)) {
            return false;
        }
        if (!Arrays.equals(stringsNull, that.stringsNull)) {
            return false;
        }
        if (!Arrays.equals(bytesFully, that.bytesFully)) {
            return false;
        }
        if (!Arrays.equals(bytesOffset, that.bytesOffset)) {
            return false;
        }
        if (!Arrays.equals(strChars, that.strChars)) {
            return false;
        }
        if (!Arrays.equals(strBytes, that.strBytes)) {
            return false;
        }
        if (portableObject != null ? !portableObject.equals(that.portableObject) : that.portableObject != null) {
            return false;
        }
        if (identifiedDataSerializableObject != null
                ? !identifiedDataSerializableObject.equals(that.identifiedDataSerializableObject)
                : that.identifiedDataSerializableObject != null) {
            return false;
        }
        if (customStreamSerializableObject != null
                ? !customStreamSerializableObject.equals(that.customStreamSerializableObject)
                : that.customStreamSerializableObject != null) {
            return false;
        }
        if (customByteArraySerializableObject != null
                ? !customByteArraySerializableObject.equals(that.customByteArraySerializableObject)
                : that.customByteArraySerializableObject != null) {
            return false;
        }
        return !(data != null ? !data.equals(that.data) : that.data != null);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (bool ? 1 : 0);
        result = 31 * result + (int) b;
        result = 31 * result + (int) c;
        temp = Double.doubleToLongBits(d);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) s;
        result = 31 * result + (f != +0.0f ? Float.floatToIntBits(f) : 0);
        result = 31 * result + i;
        result = 31 * result + (int) (l ^ (l >>> 32));
        result = 31 * result + (str != null ? str.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(booleans);
        result = 31 * result + Arrays.hashCode(bytes);
        result = 31 * result + Arrays.hashCode(chars);
        result = 31 * result + Arrays.hashCode(doubles);
        result = 31 * result + Arrays.hashCode(shorts);
        result = 31 * result + Arrays.hashCode(floats);
        result = 31 * result + Arrays.hashCode(ints);
        result = 31 * result + Arrays.hashCode(longs);
        result = 31 * result + Arrays.hashCode(strings);
        result = 31 * result + Arrays.hashCode(booleansNull);
        result = 31 * result + Arrays.hashCode(bytesNull);
        result = 31 * result + Arrays.hashCode(charsNull);
        result = 31 * result + Arrays.hashCode(doublesNull);
        result = 31 * result + Arrays.hashCode(shortsNull);
        result = 31 * result + Arrays.hashCode(floatsNull);
        result = 31 * result + Arrays.hashCode(intsNull);
        result = 31 * result + Arrays.hashCode(longsNull);
        result = 31 * result + Arrays.hashCode(stringsNull);
        result = 31 * result + (int) byteSize;
        result = 31 * result + Arrays.hashCode(bytesFully);
        result = 31 * result + Arrays.hashCode(bytesOffset);
        result = 31 * result + Arrays.hashCode(strChars);
        result = 31 * result + Arrays.hashCode(strBytes);
        result = 31 * result + unsignedByte;
        result = 31 * result + unsignedShort;
        result = 31 * result + (portableObject != null ? portableObject.hashCode() : 0);
        result = 31 * result + (identifiedDataSerializableObject != null ? identifiedDataSerializableObject.hashCode() : 0);
        result = 31 * result + (customStreamSerializableObject != null ? customStreamSerializableObject.hashCode() : 0);
        result = 31 * result + (customByteArraySerializableObject != null ? customByteArraySerializableObject.hashCode() : 0);
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AnIdentifiedDataSerializable";
    }
}

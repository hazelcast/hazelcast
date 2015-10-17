package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * Sample DataSerializable for testing internal constant serializers
 */
public class SerializationV1Portable implements Portable{


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


    public SerializationV1Portable() {
    }

    public SerializationV1Portable(byte aByte, boolean aBoolean, char character, short aShort, int integer, long aLong,
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
        out.writeUTF("9", string);

        out.writeByteArray("a1", bytes);
        out.writeBooleanArray("a2", booleans);
        out.writeCharArray("a3", chars);
        out.writeShortArray("a4", shorts);
        out.writeIntArray("a5", ints);
        out.writeLongArray("a6", longs);
        out.writeFloatArray("a7", floats);
        out.writeDoubleArray("a8", doubles);
        out.writeUTFArray("a9", strings);
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
        this.string = in.readUTF("9");

        this.bytes = in.readByteArray("a1");
        this.booleans = in.readBooleanArray("a2");
        this.chars = in.readCharArray("a3");
        this.shorts = in.readShortArray("a4");
        this.ints = in.readIntArray("a5");
        this.longs = in.readLongArray("a6");
        this.floats = in.readFloatArray("a7");
        this.doubles = in.readDoubleArray("a8");
        this.strings = in.readUTFArray("a9");
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
        if(!Arrays.equals(strings, that.strings)) {
            return false;
        }
        return true;
    }

}

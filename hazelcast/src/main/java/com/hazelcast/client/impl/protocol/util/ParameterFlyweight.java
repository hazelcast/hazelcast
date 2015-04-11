package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultData;

import java.nio.ByteBuffer;

import static com.hazelcast.client.impl.protocol.util.BitUtil.SIZE_OF_BYTE;
import static com.hazelcast.client.impl.protocol.util.BitUtil.SIZE_OF_DOUBLE;
import static com.hazelcast.client.impl.protocol.util.BitUtil.SIZE_OF_FLOAT;
import static com.hazelcast.client.impl.protocol.util.BitUtil.SIZE_OF_INT;
import static com.hazelcast.client.impl.protocol.util.BitUtil.SIZE_OF_LONG;
import static com.hazelcast.client.impl.protocol.util.BitUtil.SIZE_OF_SHORT;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Parameter Flyweight
 */
public class ParameterFlyweight
        extends Flyweight {

    private int index;

    public ParameterFlyweight() {
        super();
    }

    public ParameterFlyweight(ByteBuffer buffer, int offset) {
        super(buffer,offset);
    }

    public ParameterFlyweight wrap(final ByteBuffer buffer) {
        return wrap(buffer, 0);
    }

    public ParameterFlyweight wrap(final ByteBuffer buffer, final int offset) {
        super.wrap(buffer, offset);
        this.index = offset;
        return this;
    }

    public int index() {
        return index;
    }

    public ParameterFlyweight index(int index) {
        this.index = index;
        return this;
    }

    //region SET Overloads
    public ParameterFlyweight set(boolean value) {
        buffer.putByte(index, (byte) (value ? 1 : 0));
        index += SIZE_OF_BYTE;
        return this;
    }

    public ParameterFlyweight set(byte value) {
        buffer.putByte(index, value);
        index += SIZE_OF_BYTE;
        return this;
    }

    public ParameterFlyweight set(short value) {
        buffer.putShort(index, value, LITTLE_ENDIAN);
        index += SIZE_OF_SHORT;
        return this;
    }

    public ParameterFlyweight set(int value) {
        buffer.putInt(index, value, LITTLE_ENDIAN);
        index += SIZE_OF_INT;
        return this;
    }

    public ParameterFlyweight set(long value) {
        buffer.putLong(index, value, LITTLE_ENDIAN);
        index += SIZE_OF_LONG;
        return this;
    }

    public ParameterFlyweight set(float value) {
        buffer.putFloat(index, value, LITTLE_ENDIAN);
        index += SIZE_OF_FLOAT;
        return this;
    }

    public ParameterFlyweight set(double value) {
        buffer.putDouble(index, value, LITTLE_ENDIAN);
        index += SIZE_OF_DOUBLE;
        return this;
    }

    public ParameterFlyweight set(String value) {
        index += buffer.putStringUtf8(index, value, LITTLE_ENDIAN);
        return this;
    }

    public ParameterFlyweight set(final byte[] value) {
        final int length = value != null ? value.length : 0;
        set(length);
        if(length > 0) {
            buffer.putBytes(index, value);
            index += length;
        }
        return this;
    }

    //endregion SET Overloads

    //region GET Overloads
    public boolean getBoolean() {
        byte result = buffer.getByte(index);
        index += SIZE_OF_BYTE;
        return result != 0;
    }


    public byte getByte() {
        byte result = buffer.getByte(index);
        index += SIZE_OF_BYTE;
        return result;
    }

    public short getShort() {
        short result = buffer.getShort(index, LITTLE_ENDIAN);
        index += SIZE_OF_SHORT;
        return result;
    }

    public int getInt() {
        int result = buffer.getInt(index, LITTLE_ENDIAN);
        index += SIZE_OF_INT;
        return result;
    }

    public long getLong() {
        long result = buffer.getLong(index, LITTLE_ENDIAN);
        index += SIZE_OF_LONG;
        return result;
    }

    public float getFloat() {
        float result = buffer.getFloat(index, LITTLE_ENDIAN);
        index += SIZE_OF_FLOAT;
        return result;
    }

    public double getDouble() {
        double result = buffer.getDouble(index, LITTLE_ENDIAN);
        index += SIZE_OF_DOUBLE;
        return result;
    }

    public String getStringUtf8() {
        final int length = buffer.getInt(index, LITTLE_ENDIAN);
        String result = buffer.getStringUtf8(index, length);
        index += length + SIZE_OF_INT;
        return result;
    }

    public byte[] getByteArray() {
        final int length = buffer.getInt(index, LITTLE_ENDIAN);
        index += SIZE_OF_INT;
        byte[] result = new byte[length];
        buffer.getBytes(index, result);
        index += length;
        return result;
    }

    public Data getData() {
        return new DefaultData(getByteArray());
    }
    //endregion GET Overloads

}

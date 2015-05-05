package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    public ParameterFlyweight(byte[] buffer, int offset, int length) {
        super(buffer, offset, length);
    }

    public ParameterFlyweight(MutableDirectBuffer buffer, int offset) {
        super(buffer, offset);
    }

    public ParameterFlyweight wrap(byte[] buffer) {
        return wrap(buffer, 0, buffer.length);
    }

    public ParameterFlyweight wrap(byte[] buffer, int offset, int length) {
        super.wrap(buffer, offset, length);
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

    public ParameterFlyweight set(Data data) {
        final byte[] bytes = data.toByteArray();
        set(bytes);
        return this;
    }

    public ParameterFlyweight set(final byte[] value) {
        final int length = value != null ? value.length : 0;
        set(length);
        if (length > 0) {
            buffer.putBytes(index, value);
            index += length;
        }
        return this;
    }

    public ParameterFlyweight set(final Collection<Data> value) {
        final int length = value != null ? value.size() : 0;
        set(length);
        if(length > 0) {
            for(Data v:value){
                set(v);
            }
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

    public List<Data> getDataList() {
        final int length = buffer.getInt(index, LITTLE_ENDIAN);
        index += SIZE_OF_INT;
        final List<Data> result = new ArrayList<Data>();
        for(int i=0; i <length; i++) {
            result.add(getData());
        }
        return result;
    }

    public Set<Data> getDataSet() {
        final int length = buffer.getInt(index, LITTLE_ENDIAN);
        index += SIZE_OF_INT;
        final Set<Data> result = new HashSet<Data>();
        for(int i=0; i <length; i++) {
            result.add(getData());
        }
        return result;
    }

    //endregion GET Overloads

}

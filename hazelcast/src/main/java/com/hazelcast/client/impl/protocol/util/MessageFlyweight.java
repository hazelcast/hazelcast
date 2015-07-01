package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.DefaultData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Parameter Flyweight
 */
public class MessageFlyweight {
    /**
     * Long mask
     */
    private static final long LONG_MASK = 0x00000000FFFFFFFFL;
    /**
     * Int mask
     */
    private static final int INT_MASK = 0x0000FFFF;
    /**
     * Short mask
     */
    private static final short SHORT_MASK = 0x00FF;

    private int offset; //initialized in wrap method by user , does not change.
    private int index; //starts from zero, incremented each tome something set to buffer
    protected ClientProtocolBuffer buffer;


    public MessageFlyweight() {
        offset = 0;
    }

    public MessageFlyweight wrap(ClientProtocolBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
        this.index = 0;
        return this;
    }

    public int index() {
        return index;
    }

    public MessageFlyweight index(int index) {
        this.index = index;
        return this;
    }

    public ClientProtocolBuffer buffer() {
        return buffer;
    }

    //region SET Overloads
    public MessageFlyweight set(boolean value) {
        buffer.putByte(index + offset, (byte) (value ? 1 : 0));
        index += Bits.BYTE_SIZE_IN_BYTES;
        return this;
    }

    public MessageFlyweight set(int value) {
        buffer.putInt(index + offset, value);
        index += Bits.INT_SIZE_IN_BYTES;
        return this;
    }

    public MessageFlyweight set(long value) {
        buffer.putLong(index + offset, value);
        index += Bits.LONG_SIZE_IN_BYTES;
        return this;
    }

    public MessageFlyweight set(String value) {
        index += buffer.putStringUtf8(index + offset, value);
        return this;
    }

    public MessageFlyweight set(Data data) {
        final byte[] bytes = data.toByteArray();
        set(bytes);
        return this;
    }

    public MessageFlyweight set(final byte[] value) {
        final int length = value.length;
        set(length);
        buffer.putBytes(index + offset, value);
        index += length;
        return this;
    }

    public MessageFlyweight set(final Collection<Data> value) {
        final int length = value.size();
        set(length);
        for (Data v : value) {
            set(v);
        }
        return this;
    }

    //endregion SET Overloads

    //region GET Overloads
    public boolean getBoolean() {
        byte result = buffer.getByte(index + offset);
        index += Bits.BYTE_SIZE_IN_BYTES;
        return result != 0;
    }

    public int getInt() {
        int result = buffer.getInt(index + offset);
        index += Bits.INT_SIZE_IN_BYTES;
        return result;
    }

    public long getLong() {
        long result = buffer.getLong(index + offset);
        index += Bits.LONG_SIZE_IN_BYTES;
        return result;
    }

    public String getStringUtf8() {
        final int length = buffer.getInt(index + offset);
        String result = buffer.getStringUtf8(index + offset, length);
        index += length + Bits.INT_SIZE_IN_BYTES;
        return result;
    }

    public byte[] getByteArray() {
        final int length = buffer.getInt(index + offset);
        index += Bits.INT_SIZE_IN_BYTES;
        byte[] result = new byte[length];
        buffer.getBytes(index + offset, result);
        index += length;
        return result;
    }

    public Data getData() {
        return new DefaultData(getByteArray());
    }

    public List<Data> getDataList() {
        final int length = buffer.getInt(index + offset);
        index += Bits.INT_SIZE_IN_BYTES;
        final List<Data> result = new ArrayList<Data>();
        for (int i = 0; i < length; i++) {
            result.add(getData());
        }
        return result;
    }

    public Set<Data> getDataSet() {
        final int length = buffer.getInt(index + offset);
        index += Bits.INT_SIZE_IN_BYTES;
        final Set<Data> result = new HashSet<Data>();
        for (int i = 0; i < length; i++) {
            result.add(getData());
        }
        return result;
    }

    //endregion GET Overloads

    protected int int32Get(int index) {
        return buffer.getInt(index + offset);
    }

    protected void int32Set(int index, int length) {
        buffer.putInt(index + offset, length);
    }

    protected short uint8Get(int index) {
        return (short) (buffer.getByte(index + offset) & SHORT_MASK);
    }

    protected void uint8Put(int index, short value) {
        buffer.putByte(index + offset, (byte) value);
    }

    protected int uint16Get(int index) {
        return buffer.getShort(index + offset) & INT_MASK;
    }

    protected void uint16Put(int index, int value) {
        buffer.putShort(index + offset, (short) value);
    }

    protected long uint32Get(int index) {
        return buffer.getInt(index + offset) & LONG_MASK;
    }

    protected void uint32Put(int index, long value) {
        buffer.putInt(index + offset, (int) value);
    }

}

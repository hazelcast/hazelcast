/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    protected ClientProtocolBuffer buffer;
    //initialized in wrap method by user, does not change.
    private int offset;
    //starts from zero, incremented each time something set to buffer
    private int index;

    public MessageFlyweight() {
        offset = 0;
    }

    public MessageFlyweight wrap(byte[] buffer, int offset, boolean useUnsafe) {
        this.buffer = useUnsafe ? new UnsafeBuffer(buffer) : new SafeBuffer(buffer);
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

    public MessageFlyweight set(byte value) {
        buffer.putByte(index + offset, value);
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
        int length = data.totalSize();
        set(length);
        data.copyTo(buffer.byteArray(), index);
        index += length;
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

    public MessageFlyweight set(final Map.Entry<Data, Data> entry) {
        return set(entry.getKey()).set(entry.getValue());
    }

    //endregion SET Overloads

    //region GET Overloads
    public boolean getBoolean() {
        byte result = buffer.getByte(index + offset);
        index += Bits.BYTE_SIZE_IN_BYTES;
        return result != 0;
    }

    public byte getByte() {
        byte result = buffer.getByte(index + offset);
        index += Bits.BYTE_SIZE_IN_BYTES;
        return result;
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
        return new HeapData(getByteArray());
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
    //endregion GET Overloads

    protected int int32Get(int index) {
        return buffer.getInt(index + offset);
    }

    protected void int32Set(int index, int value) {
        buffer.putInt(index + offset, value);
    }

    protected long int64Get(int index) {
        return buffer.getLong(index + offset);
    }

    protected void int64Set(int index, long value) {
        buffer.putLong(index + offset, value);
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

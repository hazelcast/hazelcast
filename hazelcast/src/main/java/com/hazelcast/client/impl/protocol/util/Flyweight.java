/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.client.impl.protocol.util.BitUtil.BYTE_MASK;
import static com.hazelcast.client.impl.protocol.util.BitUtil.INT_MASK;
import static com.hazelcast.client.impl.protocol.util.BitUtil.LONG_MASK;

/**
 * Parent class for flyweight implementations in the messaging protocol.
 */
public class Flyweight {

    private static final byte[] EMPTY_BUFFER = new byte[0];

    protected final MutableDirectBuffer buffer = new UnsafeBuffer(EMPTY_BUFFER);
    private int offset;

    public Flyweight wrap(final byte[] buffer) {
        this.buffer.wrap(buffer);
        this.offset = 0;
        return this;
    }

    public Flyweight wrap(final ByteBuffer buffer) {
        return wrap(buffer, 0);
    }

    public Flyweight wrap(final ByteBuffer buffer, final int offset) {
        this.buffer.wrap(buffer);
        this.offset = offset;
        return this;
    }

    public Flyweight wrap(final MutableDirectBuffer buffer) {
        return wrap(buffer, 0);
    }

    public Flyweight wrap(final MutableDirectBuffer buffer, final int offset) {
        this.buffer.wrap(buffer);
        this.offset = offset;
        return this;
    }

    public MutableDirectBuffer buffer() {
        return buffer;
    }

    public int offset() {
        return offset;
    }

    public void offset(final int offset) {
        this.offset = offset;
    }

    //region PUT/GET helpers
    protected short uint8Get(final int offset) {
        return (short) (buffer.getByte(offset) & BYTE_MASK);
    }

    protected void uint8Put(final int offset, final short value) {
        buffer.putByte(offset, (byte) value);
    }

    protected int uint16Get(final int offset, final ByteOrder byteOrder) {
        return buffer.getShort(offset, byteOrder) & INT_MASK;
    }

    protected void uint16Put(final int offset, final int value, final ByteOrder byteOrder) {
        buffer.putShort(offset, (short) value, byteOrder);
    }

    protected long uint32Get(final int offset, final ByteOrder byteOrder) {
        return buffer.getInt(offset, byteOrder) & LONG_MASK;
    }

    protected void uint32Put(final int offset, final long value, final ByteOrder byteOrder) {
        buffer.putInt(offset, (int) value, byteOrder);
    }

    //endregion PUT/GET helpers

}

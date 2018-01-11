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

import com.hazelcast.nio.Bits;
import com.hazelcast.util.collection.ArrayUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;

/**
 * Supports regular, byte ordered, access to an underlying buffer.
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class UnsafeBuffer implements ClientProtocolBuffer {
    private static final String DISABLE_BOUNDS_CHECKS_PROP_NAME = "hazelcast.disable.bounds.checks";
    private static final boolean SHOULD_BOUNDS_CHECK = !Boolean.getBoolean(DISABLE_BOUNDS_CHECKS_PROP_NAME);

    private static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();
    private static final ByteOrder PROTOCOL_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
    private static final long ARRAY_BASE_OFFSET = MEM.arrayBaseOffset(byte[].class);

    private byte[] byteArray;
    private long addressOffset;

    private int capacity;

    /**
     * Attach a view to a byte[] for providing direct access.
     *
     * @param buffer The buffer to which the view is attached.
     */
    public UnsafeBuffer(final byte[] buffer) {
        wrap(buffer);
    }

    @Override
    public void wrap(final byte[] buffer) {
        addressOffset = ARRAY_BASE_OFFSET;
        capacity = buffer.length;
        byteArray = buffer;
    }

    @Override
    public byte[] byteArray() {
        return byteArray;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    ///////////////////////////////////////////////////////////////////////////

    @Override
    public long getLong(final int index) {
        boundsCheck(index, Bits.LONG_SIZE_IN_BYTES);

        long bits = MEM.getLong(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != PROTOCOL_BYTE_ORDER) {
            bits = Long.reverseBytes(bits);
        }

        return bits;
    }

    @Override
    public void putLong(final int index, final long value) {
        boundsCheck(index, Bits.LONG_SIZE_IN_BYTES);

        long bits = value;
        if (NATIVE_BYTE_ORDER != PROTOCOL_BYTE_ORDER) {
            bits = Long.reverseBytes(bits);
        }

        MEM.putLong(byteArray, addressOffset + index, bits);
    }

    ///////////////////////////////////////////////////////////////////////////

    @Override
    public int getInt(final int index) {
        boundsCheck(index, Bits.INT_SIZE_IN_BYTES);

        int bits = MEM.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != PROTOCOL_BYTE_ORDER) {
            bits = Integer.reverseBytes(bits);
        }

        return bits;
    }

    @Override
    public void putInt(final int index, final int value) {
        boundsCheck(index, Bits.INT_SIZE_IN_BYTES);

        int bits = value;
        if (NATIVE_BYTE_ORDER != PROTOCOL_BYTE_ORDER) {
            bits = Integer.reverseBytes(bits);
        }

        MEM.putInt(byteArray, addressOffset + index, bits);
    }


    ///////////////////////////////////////////////////////////////////////////

    @Override
    public short getShort(final int index) {
        boundsCheck(index, Bits.SHORT_SIZE_IN_BYTES);

        short bits = MEM.getShort(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != PROTOCOL_BYTE_ORDER) {
            bits = Short.reverseBytes(bits);
        }

        return bits;
    }

    @Override
    public void putShort(final int index, final short value) {
        boundsCheck(index, Bits.SHORT_SIZE_IN_BYTES);

        short bits = value;
        if (NATIVE_BYTE_ORDER != PROTOCOL_BYTE_ORDER) {
            bits = Short.reverseBytes(bits);
        }

        MEM.putShort(byteArray, addressOffset + index, bits);
    }

    @Override
    public byte getByte(final int index) {
        boundsCheck(index, Bits.BYTE_SIZE_IN_BYTES);

        return MEM.getByte(byteArray, addressOffset + index);
    }

    @Override
    public void putByte(final int index, final byte value) {
        boundsCheck(index, Bits.BYTE_SIZE_IN_BYTES);

        MEM.putByte(byteArray, addressOffset + index, value);
    }

    @Override
    public void getBytes(final int index, final byte[] dst) {
        getBytes(index, dst, 0, dst.length);
    }

    @Override
    public void getBytes(final int index, final byte[] dst, final int offset, final int length) {
        boundsCheck(index, length);
        boundsCheck(dst, offset, length);

        MEM.copyMemory(byteArray, addressOffset + index, dst, ARRAY_BASE_OFFSET + offset, length);
    }

    @Override
    public void putBytes(final int index, final byte[] src) {
        putBytes(index, src, 0, src.length);
    }

    @Override
    public void putBytes(final int index, final byte[] src, final int offset, final int length) {
        boundsCheck(index, length);
        boundsCheck(src, offset, length);

        MEM.copyMemory(src, ARRAY_BASE_OFFSET + offset, byteArray, addressOffset + index, length);
    }

    @Override
    public void putBytes(int index, ByteBuffer src, int length) {
        if (src.isDirect()) {
            src.get(byteArray, index, length);
        } else {
            // to prevent causing any regressions, in case of a heap buffer, we leave the original copy logic in place.
            putBytes(index, src.array(), src.position(), length);
            src.position(src.position() + length);
        }
    }

    @Override
    public String getStringUtf8(final int offset, final int length) {
        final byte[] stringInBytes = new byte[length];
        getBytes(offset + Bits.INT_SIZE_IN_BYTES, stringInBytes);

        return new String(stringInBytes, Bits.UTF_8);
    }

    @Override
    public int putStringUtf8(final int index, final String value) {
        return putStringUtf8(index, value, Integer.MAX_VALUE);
    }

    @Override
    public int putStringUtf8(final int index, final String value, final int maxEncodedSize) {
        final byte[] bytes = value.getBytes(Bits.UTF_8);
        if (bytes.length > maxEncodedSize) {
            throw new IllegalArgumentException("Encoded string larger than maximum size: " + maxEncodedSize);
        }

        putInt(index, bytes.length);
        putBytes(index + Bits.INT_SIZE_IN_BYTES, bytes);

        return Bits.INT_SIZE_IN_BYTES + bytes.length;
    }

    ///////////////////////////////////////////////////////////////////////////

    private void boundsCheck(final int index, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            ArrayUtils.boundsCheck(capacity, index, length);
        }
    }

    private static void boundsCheck(final byte[] buffer, final int index, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            ArrayUtils.boundsCheck(buffer.length, index, length);
        }
    }
}

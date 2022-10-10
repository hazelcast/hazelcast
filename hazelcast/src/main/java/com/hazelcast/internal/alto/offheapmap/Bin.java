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

package com.hazelcast.internal.alto.offheapmap;

import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class Bin {

    private IOBuffer buf;
    private ByteBuffer buffer;
    private int dataOffset;
    private int size;

    // for debugging only because it creates litter!
    public byte[] bytes() {
        byte[] bytes = new byte[size];
        int pos = buffer.position();
        buffer.position(dataOffset);
        buffer.get(bytes);
        buffer.position(pos);
        return bytes;
    }

    public void init(IOBuffer buf) {
        this.buf = buf;
        this.buffer = buf.byteBuffer();
        this.size = buf.readInt();
        this.dataOffset = buf.position();
        buf.incPosition(size);
    }

    // Todo: very inefficient.
    public boolean unsafeEquals(Unsafe unsafe, long address) {
        int keyLength = unsafe.getInt(address);
        if (keyLength != size) {
            return false;
        }

        address += BYTES_INT;

        ByteBuffer buffer = buf.byteBuffer();
        for (int k = 0; k < size; k++) {
            if (buffer.get(dataOffset + k) != unsafe.getByte(address + k)) {
                return false;
            }
        }

        return true;
    }

    public int hash() {
        int result = 1;
        for (int k = 0; k < size; k++) {
            result = 31 * result + buffer.get(dataOffset + k);
        }

        return result;
    }

    /**
     * Copies the content of Bin to offheap.
     *
     * @param unsafe
     * @param dstAddress
     */
    public void copyTo(Unsafe unsafe, long dstAddress) {
        unsafe.putInt(dstAddress, size);
        dstAddress += INT_SIZE_IN_BYTES;

        ByteBuffer buffer = buf.byteBuffer();
        if (buffer.hasArray()) {
            unsafe.copyMemory(buffer.array(), ARRAY_BYTE_BASE_OFFSET + dataOffset, null, dstAddress, size);
        } else {
            unsafe.copyMemory(addressOf(buffer) + dataOffset, dstAddress, size);
        }
    }

    /**
     * The number of bytes taken up by the key itself (excluding the size field).
     *
     * @return
     */
    public int size() {
        return size;
    }

    public void clear() {
        this.buf = null;
    }
}

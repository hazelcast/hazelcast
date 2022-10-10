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
import com.hazelcast.internal.tpc.util.BufferUtil;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class Bout {

    private IOBuffer buf;
    private int size;
    private int startPos;

    public void init(IOBuffer buf) {
        this.buf = buf;
        this.startPos = buf.position();
        this.size = -1;
    }

    // for testing only
    public byte[] bytes() {
        if (buf.getInt(startPos) == -1) {
            return null;
        } else {
            byte[] bytes = new byte[size];
            for (int k = 0; k < bytes.length; k++) {
                bytes[k] = buf.getByte(startPos + BYTES_INT + k);
            }
            return bytes;
        }
    }

    public void writeNull() {
        buf.writeInt(-1);
    }

    public void writeFrom(Unsafe unsafe, long srcAddress) {
        size = unsafe.getInt(srcAddress);
        buf.ensureRemaining(BYTES_INT + size);
        buf.writeInt(size);

        srcAddress += BYTES_INT;

        ByteBuffer buffer = buf.byteBuffer();
        if (buffer.hasArray()) {
            unsafe.copyMemory(null, srcAddress, buffer.array(), ARRAY_BYTE_BASE_OFFSET + startPos + BYTES_INT, size);
        } else {
            unsafe.copyMemory(srcAddress, BufferUtil.addressOf(buffer) + startPos + BYTES_INT, size);
        }
        buf.incPosition(size);
    }

    public void clear() {
        this.buf = null;
    }
}

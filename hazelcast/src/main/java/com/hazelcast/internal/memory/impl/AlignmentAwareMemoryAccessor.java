/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.MemoryAccessor;

import static com.hazelcast.internal.memory.impl.AlignmentUtil.is2BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.is4BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.is8BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.BIG_ENDIAN;

/**
 * <p>
 * Aligned {@link MemoryAccessor} which checks for and handles unaligned memory access
 * by splitting a larger-size memory operation into several smaller-size ones
 * (which have finer-grained alignment requirements).
 * </p><p>
 * A few notes on this implementation:
 * <ul>
 *      <li>
 *        There is no atomicity guarantee for unaligned memory accesses.
 *        In fact, even on platforms which support unaligned memory accesses,
 *        there is no guarantee for atomicity when there is unaligned memory accesses.
 *        On later Intel processors, unaligned access within the cache line is atomic,
 *        but access that straddles cache lines is not.
 *        See http://psy-lob-saw.blogspot.com.tr/2013/07/atomicity-of-unaligned-memory-access-in.html
 *        for more details.
 *      </li>
 *      <li>Unaligned memory access is not supported for CAS operations. </li>
 *      <li>Unaligned memory access is not supported for ordered writes. </li>
 * </ul>
 * </p>
 */
@SuppressWarnings({"checkstyle:magicnumber" })
public class AlignmentAwareMemoryAccessor extends StandardMemoryAccessor {

    public AlignmentAwareMemoryAccessor() {
        if (!AVAILABLE) {
            throw new IllegalStateException(getClass().getName() + " can only be used only when Unsafe is available!");
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void copyMemory(long srcAddress, long destAddress, long bytes) {
        // TODO Should we check and handle alignment???
        super.copyMemory(srcAddress, destAddress, bytes);
    }

    @Override
    public void setMemory(long address, long bytes, byte value) {
        // TODO Should we check and handle alignment???
        super.setMemory(address, bytes, value);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public char getChar(long address) {
        if (is2BytesAligned(address)) {
            return super.getChar(address);
        } else {
            return EndiannessUtility.readChar(address, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putChar(long address, char x) {
        if (is2BytesAligned(address)) {
            super.putChar(address, x);
        } else {
            EndiannessUtility.writeChar(address, x, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public short getShort(long address) {
        if (is2BytesAligned(address)) {
            return super.getShort(address);
        } else {
            return EndiannessUtility.readShort(address, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putShort(long address, short x) {
        if (is2BytesAligned(address)) {
            super.putShort(address, x);
        } else {
            EndiannessUtility.writeShort(address, x, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public int getInt(long address) {
        if (is4BytesAligned(address)) {
            return super.getInt(address);
        } else {
            return EndiannessUtility.readInt(address, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putInt(long address, int x) {
        if (is4BytesAligned(address)) {
            super.putInt(address, x);
        } else {
            EndiannessUtility.writeInt(address, x, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public float getFloat(long address) {
        if (is4BytesAligned(address)) {
            return super.getFloat(address);
        } else {
            return EndiannessUtility.readFloat(address, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putFloat(long address, float x) {
        if (is4BytesAligned(address)) {
            super.putFloat(address, x);
        } else {
            EndiannessUtility.writeFloat(address, x, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public long getLong(long address) {
        if (is8BytesAligned(address)) {
            return super.getLong(address);
        } else {
            return EndiannessUtility.readLong(address, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putLong(long address, long x) {
        if (is8BytesAligned(address)) {
            super.putLong(address, x);
        } else {
            EndiannessUtility.writeLong(address, x, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public double getDouble(long address) {
        if (is8BytesAligned(address)) {
            return super.getDouble(address);
        } else {
            return EndiannessUtility.readDouble(address, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putDouble(long address, double x) {
        if (is8BytesAligned(address)) {
            super.putDouble(address, x);
        } else {
            EndiannessUtility.writeDouble(address, x, BIG_ENDIAN);
        }
    }
}

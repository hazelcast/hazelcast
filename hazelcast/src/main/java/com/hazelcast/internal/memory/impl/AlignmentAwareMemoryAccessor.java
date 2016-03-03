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

import static com.hazelcast.internal.memory.impl.AlignmentUtil.IS_PLATFORM_BIG_ENDIAN;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.check2BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.check4BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.check8BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.checkReferenceAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.is2BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.is4BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.is8BytesAligned;
import static com.hazelcast.internal.memory.impl.EndiannessUtil.NATIVE_ACCESS;
import static com.hazelcast.internal.memory.impl.UnsafeUtil.UNSAFE_AVAILABLE;

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
@SuppressWarnings("checkstyle:methodcount")
public final class AlignmentAwareMemoryAccessor extends StandardMemoryAccessor {
    public static final AlignmentAwareMemoryAccessor INSTANCE = UNSAFE_AVAILABLE ? new AlignmentAwareMemoryAccessor() : null;

    private AlignmentAwareMemoryAccessor() {
        if (!UNSAFE_AVAILABLE) {
            throw new IllegalStateException(getClass().getName() + " can only be used only when Unsafe is available!");
        }
    }


    // Address-based access


    @Override
    public char getChar(long address) {
        return is2BytesAligned(address) ? super.getChar(address)
                : EndiannessUtil.readChar(NATIVE_ACCESS, null, address, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putChar(long address, char x) {
        if (is2BytesAligned(address)) {
            super.putChar(address, x);
        } else {
            EndiannessUtil.writeChar(NATIVE_ACCESS, null, address, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public short getShort(long address) {
        return is2BytesAligned(address) ? super.getShort(address)
                : EndiannessUtil.readShort(NATIVE_ACCESS, null, address, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putShort(long address, short x) {
        if (is2BytesAligned(address)) {
            super.putShort(address, x);
        } else {
            EndiannessUtil.writeShort(NATIVE_ACCESS, null, address, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public int getInt(long address) {
        return is4BytesAligned(address) ? super.getInt(address)
                : EndiannessUtil.readInt(NATIVE_ACCESS, null, address, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putInt(long address, int x) {
        if (is4BytesAligned(address)) {
            super.putInt(address, x);
        } else {
            EndiannessUtil.writeInt(NATIVE_ACCESS, null, address, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public float getFloat(long address) {
        return is4BytesAligned(address) ? super.getFloat(address)
                : EndiannessUtil.readFloat(NATIVE_ACCESS, null, address, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putFloat(long address, float x) {
        if (is4BytesAligned(address)) {
            super.putFloat(address, x);
        } else {
            EndiannessUtil.writeFloat(NATIVE_ACCESS, null, address, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public long getLong(long address) {
        return is8BytesAligned(address) ? super.getLong(address)
                : EndiannessUtil.readLong(NATIVE_ACCESS, null, address, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putLong(long address, long x) {
        if (is8BytesAligned(address)) {
            super.putLong(address, x);
        } else {
            EndiannessUtil.writeLong(NATIVE_ACCESS, null, address, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public double getDouble(long address) {
        return is8BytesAligned(address) ? super.getDouble(address)
                : EndiannessUtil.readDouble(NATIVE_ACCESS, null, address, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putDouble(long address, double x) {
        if (is8BytesAligned(address)) {
            super.putDouble(address, x);
        } else {
            EndiannessUtil.writeDouble(NATIVE_ACCESS, null, address, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }


    // Address-based concurrent operations


    @Override
    public char getCharVolatile(long address) {
        check2BytesAligned(address);
        return super.getCharVolatile(address);
    }

    @Override
    public void putCharVolatile(long address, char x) {
        check2BytesAligned(address);
        super.putCharVolatile(address, x);
    }

    @Override
    public short getShortVolatile(long address) {
        check2BytesAligned(address);
        return super.getShortVolatile(address);
    }

    @Override
    public void putShortVolatile(long address, short x) {
        check2BytesAligned(address);
        super.putShortVolatile(address, x);
    }

    @Override
    public int getIntVolatile(long address) {
        check4BytesAligned(address);
        return super.getIntVolatile(address);
    }

    @Override
    public void putIntVolatile(long address, int x) {
        check4BytesAligned(address);
        super.putIntVolatile(address, x);
    }

    @Override
    public float getFloatVolatile(long address) {
        check4BytesAligned(address);
        return super.getFloatVolatile(address);
    }

    @Override
    public void putFloatVolatile(long address, float x) {
        check4BytesAligned(address);
        super.putFloatVolatile(address, x);
    }

    @Override
    public long getLongVolatile(long address) {
        check8BytesAligned(address);
        return super.getLongVolatile(address);
    }

    @Override
    public void putLongVolatile(long address, long x) {
        check8BytesAligned(address);
        super.putLongVolatile(address, x);
    }

    @Override
    public double getDoubleVolatile(long address) {
        check8BytesAligned(address);
        return super.getDoubleVolatile(address);
    }

    @Override
    public void putDoubleVolatile(long address, double x) {
        check8BytesAligned(address);
        super.putDoubleVolatile(address, x);
    }

    @Override
    public void putOrderedInt(long address, int x) {
        check4BytesAligned(address);
        super.putOrderedInt(address, x);
    }

    @Override
    public void putOrderedLong(long address, long x) {
        check8BytesAligned(address);
        super.putOrderedLong(address, x);
    }

    @Override
    public void putOrderedObject(long address, Object x) {
        checkReferenceAligned(address);
        super.putOrderedObject(address, x);
    }

    @Override
    public boolean compareAndSwapInt(long address, int expected, int x) {
        check4BytesAligned(address);
        return super.compareAndSwapInt(address, expected, x);
    }

    @Override
    public boolean compareAndSwapLong(long address, long expected, long x) {
        check8BytesAligned(address);
        return super.compareAndSwapLong(address, expected, x);
    }

    @Override
    public boolean compareAndSwapObject(long address, Object expected, Object x) {
        checkReferenceAligned(address);
        return super.compareAndSwapObject(address, expected, x);
    }




    // Object-based access


    @Override
    public Object getObject(Object base, long offset) {
        checkReferenceAligned(offset);
        return super.getObject(base, offset);
    }

    @Override
    public void putObject(Object base, long offset, Object x) {
        checkReferenceAligned(offset);
        super.putObject(base, offset, x);
    }

    @Override
    public char getChar(Object base, long offset) {
        return is2BytesAligned(offset) ? super.getChar(base, offset)
                : EndiannessUtil.readChar(NATIVE_ACCESS, base, offset, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putChar(Object base, long offset, char x) {
        if (is2BytesAligned(offset)) {
            super.putChar(base, offset, x);
        } else {
            EndiannessUtil.writeChar(NATIVE_ACCESS, base, offset, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public short getShort(Object base, long offset) {
        return is2BytesAligned(offset) ? super.getShort(base, offset)
                : EndiannessUtil.readShort(NATIVE_ACCESS, base, offset, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putShort(Object base, long offset, short x) {
        if (is2BytesAligned(offset)) {
            super.putShort(base, offset, x);
        } else {
            EndiannessUtil.writeShort(NATIVE_ACCESS, base, offset, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public int getInt(Object base, long offset) {
        return is4BytesAligned(offset) ? super.getInt(base, offset)
                : EndiannessUtil.readInt(NATIVE_ACCESS, base, offset, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putInt(Object base, long offset, int x) {
        if (is4BytesAligned(offset)) {
            super.putInt(base, offset, x);
        } else {
            EndiannessUtil.writeInt(NATIVE_ACCESS, base, offset, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public float getFloat(Object base, long offset) {
        return is4BytesAligned(offset) ? super.getFloat(base, offset)
                : EndiannessUtil.readFloat(NATIVE_ACCESS, base, offset, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putFloat(Object base, long offset, float x) {
        if (is4BytesAligned(offset)) {
            super.putFloat(base, offset, x);
        } else {
            EndiannessUtil.writeFloat(NATIVE_ACCESS, base, offset, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public long getLong(Object base, long offset) {
        return is8BytesAligned(offset) ? super.getLong(base, offset)
                : EndiannessUtil.readLong(NATIVE_ACCESS, base, offset, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putLong(Object base, long offset, long x) {
        if (is8BytesAligned(offset)) {
            super.putLong(base, offset, x);
        } else {
            EndiannessUtil.writeLong(NATIVE_ACCESS, base, offset, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public double getDouble(Object base, long offset) {
        return is8BytesAligned(offset) ? super.getDouble(base, offset)
                : EndiannessUtil.readDouble(NATIVE_ACCESS, base, offset, IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putDouble(Object base, long offset, double x) {
        if (is8BytesAligned(offset)) {
            super.putDouble(base, offset, x);
        } else {
            EndiannessUtil.writeDouble(NATIVE_ACCESS, base, offset, x, IS_PLATFORM_BIG_ENDIAN);
        }
    }


    // Object-based concurrent operations


    @Override
    public Object getObjectVolatile(Object base, long offset) {
        checkReferenceAligned(offset);
        return super.getObjectVolatile(base, offset);
    }

    @Override
    public void putObjectVolatile(Object base, long offset, Object x) {
        checkReferenceAligned(offset);
        super.putObjectVolatile(base, offset, x);
    }

    @Override
    public char getCharVolatile(Object base, long offset) {
        check2BytesAligned(offset);
        return super.getCharVolatile(base, offset);
    }

    @Override
    public void putCharVolatile(Object base, long offset, char x) {
        check2BytesAligned(offset);
        super.putCharVolatile(base, offset, x);
    }

    @Override
    public short getShortVolatile(Object base, long offset) {
        check2BytesAligned(offset);
        return super.getShortVolatile(base, offset);
    }

    @Override
    public void putShortVolatile(Object base, long offset, short x) {
        check2BytesAligned(offset);
        super.putShortVolatile(base, offset, x);
    }

    @Override
    public int getIntVolatile(Object base, long offset) {
        check4BytesAligned(offset);
        return super.getIntVolatile(base, offset);
    }

    @Override
    public void putIntVolatile(Object base, long offset, int x) {
        check4BytesAligned(offset);
        super.putIntVolatile(base, offset, x);
    }

    @Override
    public float getFloatVolatile(Object base, long offset) {
        check4BytesAligned(offset);
        return super.getFloatVolatile(base, offset);
    }

    @Override
    public void putFloatVolatile(Object base, long offset, float x) {
        check4BytesAligned(offset);
        super.putFloatVolatile(base, offset, x);
    }

    @Override
    public long getLongVolatile(Object base, long offset) {
        check8BytesAligned(offset);
        return super.getLongVolatile(base, offset);
    }

    @Override
    public void putLongVolatile(Object base, long offset, long x) {
        check8BytesAligned(offset);
        super.putLongVolatile(base, offset, x);
    }

    @Override
    public double getDoubleVolatile(Object base, long offset) {
        check8BytesAligned(offset);
        return super.getDoubleVolatile(base, offset);
    }

    @Override
    public void putDoubleVolatile(Object base, long offset, double x) {
        check8BytesAligned(offset);
        super.putDoubleVolatile(base, offset, x);
    }

    @Override
    public void putOrderedInt(Object base, long offset, int x) {
        check4BytesAligned(offset);
        super.putOrderedInt(base, offset, x);
    }

    @Override
    public void putOrderedLong(Object base, long offset, long x) {
        check8BytesAligned(offset);
        super.putOrderedLong(base, offset, x);
    }

    @Override
    public void putOrderedObject(Object base, long offset, Object x) {
        checkReferenceAligned(offset);
        super.putOrderedObject(base, offset, x);
    }

    @Override
    public boolean compareAndSwapInt(Object base, long offset, int expected, int x) {
        check4BytesAligned(offset);
        return super.compareAndSwapInt(base, offset, expected, x);
    }

    @Override
    public boolean compareAndSwapLong(Object base, long offset, long expected, long x) {
        check8BytesAligned(offset);
        return super.compareAndSwapLong(base, offset, expected, x);
    }

    @Override
    public boolean compareAndSwapObject(Object base, long offset, Object expected, Object x) {
        checkReferenceAligned(offset);
        return super.compareAndSwapObject(base, offset, expected, x);
    }
}

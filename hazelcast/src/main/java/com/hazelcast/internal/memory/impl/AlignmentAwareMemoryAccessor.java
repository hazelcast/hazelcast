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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.MemoryAccessor;

import java.lang.reflect.Field;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.impl.UnsafeUtil.UNSAFE;
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
 */
@SuppressWarnings("checkstyle:methodcount")
public final class AlignmentAwareMemoryAccessor extends UnsafeBasedMemoryAccessor {
    public static final AlignmentAwareMemoryAccessor INSTANCE = UNSAFE_AVAILABLE ? new AlignmentAwareMemoryAccessor() : null;

    private AlignmentAwareMemoryAccessor() {
        if (!UNSAFE_AVAILABLE) {
            throw new IllegalStateException(getClass().getName() + " can only be used only when Unsafe is available!");
        }
    }

    // Address-based access

    @Override
    public boolean getBoolean(long address) {
        return UNSAFE.getBoolean(null, address);
    }

    @Override
    public void putBoolean(long address, boolean x) {
        UNSAFE.putBoolean(null, address, x);
    }

    @Override
    public byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    @Override
    public void putByte(long address, byte x) {
        UNSAFE.putByte(address, x);
    }

    @Override
    public char getChar(long address) {
        return AlignmentUtil.is2BytesAligned(address) ? UNSAFE.getChar(address)
                : EndiannessUtil.readChar(EndiannessUtil.NATIVE_ACCESS, null, address, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putChar(long address, char x) {
        if (AlignmentUtil.is2BytesAligned(address)) {
            UNSAFE.putChar(address, x);
        } else {
            EndiannessUtil.writeChar(EndiannessUtil.NATIVE_ACCESS, null, address, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public short getShort(long address) {
        return AlignmentUtil.is2BytesAligned(address) ? UNSAFE.getShort(address)
                : EndiannessUtil.readShort(EndiannessUtil.NATIVE_ACCESS, null, address, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putShort(long address, short x) {
        if (AlignmentUtil.is2BytesAligned(address)) {
            UNSAFE.putShort(address, x);
        } else {
            EndiannessUtil.writeShort(EndiannessUtil.NATIVE_ACCESS, null, address, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public int getInt(long address) {
        return AlignmentUtil.is4BytesAligned(address) ? UNSAFE.getInt(address)
                : EndiannessUtil.readInt(EndiannessUtil.NATIVE_ACCESS, null, address, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putInt(long address, int x) {
        if (AlignmentUtil.is4BytesAligned(address)) {
            UNSAFE.putInt(address, x);
        } else {
            EndiannessUtil.writeInt(EndiannessUtil.NATIVE_ACCESS, null, address, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public float getFloat(long address) {
        return AlignmentUtil.is4BytesAligned(address) ? UNSAFE.getFloat(address)
                : EndiannessUtil.readFloat(EndiannessUtil.NATIVE_ACCESS, null, address, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putFloat(long address, float x) {
        if (AlignmentUtil.is4BytesAligned(address)) {
            UNSAFE.putFloat(address, x);
        } else {
            EndiannessUtil.writeFloat(EndiannessUtil.NATIVE_ACCESS, null, address, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public long getLong(long address) {
        return AlignmentUtil.is8BytesAligned(address) ? UNSAFE.getLong(address)
                : EndiannessUtil.readLong(EndiannessUtil.NATIVE_ACCESS, null, address, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putLong(long address, long x) {
        if (AlignmentUtil.is8BytesAligned(address)) {
            UNSAFE.putLong(address, x);
        } else {
            EndiannessUtil.writeLong(EndiannessUtil.NATIVE_ACCESS, null, address, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public double getDouble(long address) {
        return AlignmentUtil.is8BytesAligned(address) ? UNSAFE.getDouble(address)
                : EndiannessUtil.readDouble(EndiannessUtil.NATIVE_ACCESS, null, address, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putDouble(long address, double x) {
        if (AlignmentUtil.is8BytesAligned(address)) {
            UNSAFE.putDouble(address, x);
        } else {
            EndiannessUtil.writeDouble(EndiannessUtil.NATIVE_ACCESS, null, address, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public void copyMemory(long srcAddress, long destAddress, long lengthBytes) {
        UNSAFE.copyMemory(srcAddress, destAddress, lengthBytes);
    }

    @Override
    public void copyFromByteArray(byte[] source, int offset, long destAddress, int length) {
        AMEM.copyFromByteArray(source, offset, destAddress, length);
    }

    @Override
    public void copyToByteArray(long srcAddress, byte[] destination, int offset, int length) {
        AMEM.copyToByteArray(srcAddress, destination, offset, length);
    }

    @Override
    public void setMemory(long address, long lengthBytes, byte value) {
        UNSAFE.setMemory(address, lengthBytes, value);
    }

    // Address-based concurrent operations

    @Override
    public boolean getBooleanVolatile(long address) {
        return UNSAFE.getBooleanVolatile(null, address);
    }

    @Override
    public void putBooleanVolatile(long address, boolean x) {
        UNSAFE.putBooleanVolatile(null, address, x);
    }

    @Override
    public byte getByteVolatile(long address) {
        return UNSAFE.getByteVolatile(null, address);
    }

    @Override
    public void putByteVolatile(long address, byte x) {
        UNSAFE.putByteVolatile(null, address, x);
    }

    @Override
    public char getCharVolatile(long address) {
        AlignmentUtil.check2BytesAligned(address);
        return UNSAFE.getCharVolatile(null, address);
    }

    @Override
    public void putCharVolatile(long address, char x) {
        AlignmentUtil.check2BytesAligned(address);
        UNSAFE.putCharVolatile(null, address, x);
    }

    @Override
    public short getShortVolatile(long address) {
        AlignmentUtil.check2BytesAligned(address);
        return UNSAFE.getShortVolatile(null, address);
    }

    @Override
    public void putShortVolatile(long address, short x) {
        AlignmentUtil.check2BytesAligned(address);
        UNSAFE.putShortVolatile(null, address, x);
    }

    @Override
    public int getIntVolatile(long address) {
        AlignmentUtil.check4BytesAligned(address);
        return UNSAFE.getIntVolatile(null, address);
    }

    @Override
    public void putIntVolatile(long address, int x) {
        AlignmentUtil.check4BytesAligned(address);
        UNSAFE.putIntVolatile(null, address, x);
    }

    @Override
    public float getFloatVolatile(long address) {
        AlignmentUtil.check4BytesAligned(address);
        return UNSAFE.getFloatVolatile(null, address);
    }

    @Override
    public void putFloatVolatile(long address, float x) {
        AlignmentUtil.check4BytesAligned(address);
        UNSAFE.putFloatVolatile(null, address, x);
    }

    @Override
    public long getLongVolatile(long address) {
        AlignmentUtil.check8BytesAligned(address);
        return UNSAFE.getLongVolatile(null, address);
    }

    @Override
    public void putLongVolatile(long address, long x) {
        AlignmentUtil.check8BytesAligned(address);
        UNSAFE.putLongVolatile(null, address, x);
    }

    @Override
    public double getDoubleVolatile(long address) {
        AlignmentUtil.check8BytesAligned(address);
        return UNSAFE.getDoubleVolatile(null, address);
    }

    @Override
    public void putDoubleVolatile(long address, double x) {
        AlignmentUtil.check8BytesAligned(address);
        UNSAFE.putDoubleVolatile(null, address, x);
    }

    @Override
    public void putOrderedInt(long address, int x) {
        AlignmentUtil.check4BytesAligned(address);
        UNSAFE.putOrderedInt(null, address, x);
    }

    @Override
    public void putOrderedLong(long address, long x) {
        AlignmentUtil.check8BytesAligned(address);
        UNSAFE.putOrderedLong(null, address, x);
    }

    @Override
    public void putOrderedObject(long address, Object x) {
        AlignmentUtil.checkReferenceAligned(address);
        UNSAFE.putOrderedObject(null, address, x);
    }

    @Override
    public boolean compareAndSwapInt(long address, int expected, int x) {
        AlignmentUtil.check4BytesAligned(address);
        return UNSAFE.compareAndSwapInt(null, address, expected, x);
    }

    @Override
    public boolean compareAndSwapLong(long address, long expected, long x) {
        AlignmentUtil.check8BytesAligned(address);
        return UNSAFE.compareAndSwapLong(null, address, expected, x);
    }

    @Override
    public boolean compareAndSwapObject(long address, Object expected, Object x) {
        AlignmentUtil.checkReferenceAligned(address);
        return UNSAFE.compareAndSwapObject(null, address, expected, x);
    }

    // Object-based access

    @Override
    public boolean getBoolean(Object base, long offset) {
        return UNSAFE.getBoolean(base, offset);
    }

    @Override
    public void putBoolean(Object base, long offset, boolean x) {
        UNSAFE.putBoolean(base, offset, x);
    }

    @Override
    public byte getByte(Object base, long offset) {
        return UNSAFE.getByte(base, offset);
    }

    @Override
    public void putByte(Object base, long offset, byte x) {
        UNSAFE.putByte(base, offset, x);
    }

    @Override
    public long objectFieldOffset(Field field) {
        return UNSAFE.objectFieldOffset(field);
    }

    @Override
    public int arrayBaseOffset(Class<?> arrayClass) {
        return UNSAFE.arrayBaseOffset(arrayClass);
    }

    @Override
    public int arrayIndexScale(Class<?> arrayClass) {
        return UNSAFE.arrayIndexScale(arrayClass);
    }

    @Override
    public void copyMemory(Object srcObj, long srcOffset, Object destObj, long destOffset, long lengthBytes) {
        UNSAFE.copyMemory(srcObj, srcOffset, destObj, destOffset, lengthBytes);
    }

    @Override
    public Object getObject(Object base, long offset) {
        AlignmentUtil.checkReferenceAligned(offset);
        return UNSAFE.getObject(base, offset);
    }

    @Override
    public void putObject(Object base, long offset, Object x) {
        AlignmentUtil.checkReferenceAligned(offset);
        UNSAFE.putObject(base, offset, x);
    }

    @Override
    public char getChar(Object base, long offset) {
        return AlignmentUtil.is2BytesAligned(offset) ? UNSAFE.getChar(base, offset)
                : EndiannessUtil.readChar(EndiannessUtil.NATIVE_ACCESS, base, offset, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putChar(Object base, long offset, char x) {
        if (AlignmentUtil.is2BytesAligned(offset)) {
            UNSAFE.putChar(base, offset, x);
        } else {
            EndiannessUtil.writeChar(EndiannessUtil.NATIVE_ACCESS, base, offset, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public short getShort(Object base, long offset) {
        return AlignmentUtil.is2BytesAligned(offset) ? UNSAFE.getShort(base, offset)
                : EndiannessUtil.readShort(EndiannessUtil.NATIVE_ACCESS, base, offset, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putShort(Object base, long offset, short x) {
        if (AlignmentUtil.is2BytesAligned(offset)) {
            UNSAFE.putShort(base, offset, x);
        } else {
            EndiannessUtil.writeShort(EndiannessUtil.NATIVE_ACCESS, base, offset, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public int getInt(Object base, long offset) {
        return AlignmentUtil.is4BytesAligned(offset) ? UNSAFE.getInt(base, offset)
                : EndiannessUtil.readInt(EndiannessUtil.NATIVE_ACCESS, base, offset, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putInt(Object base, long offset, int x) {
        if (AlignmentUtil.is4BytesAligned(offset)) {
            UNSAFE.putInt(base, offset, x);
        } else {
            EndiannessUtil.writeInt(EndiannessUtil.NATIVE_ACCESS, base, offset, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public float getFloat(Object base, long offset) {
        return AlignmentUtil.is4BytesAligned(offset) ? UNSAFE.getFloat(base, offset)
                : EndiannessUtil.readFloat(EndiannessUtil.NATIVE_ACCESS, base, offset, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putFloat(Object base, long offset, float x) {
        if (AlignmentUtil.is4BytesAligned(offset)) {
            UNSAFE.putFloat(base, offset, x);
        } else {
            EndiannessUtil.writeFloat(EndiannessUtil.NATIVE_ACCESS, base, offset, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public long getLong(Object base, long offset) {
        return AlignmentUtil.is8BytesAligned(offset) ? UNSAFE.getLong(base, offset)
                : EndiannessUtil.readLong(EndiannessUtil.NATIVE_ACCESS, base, offset, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putLong(Object base, long offset, long x) {
        if (AlignmentUtil.is8BytesAligned(offset)) {
            UNSAFE.putLong(base, offset, x);
        } else {
            EndiannessUtil.writeLong(EndiannessUtil.NATIVE_ACCESS, base, offset, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    @Override
    public double getDouble(Object base, long offset) {
        return AlignmentUtil.is8BytesAligned(offset) ? UNSAFE.getDouble(base, offset)
                : EndiannessUtil.readDouble(EndiannessUtil.NATIVE_ACCESS, base, offset, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
    }

    @Override
    public void putDouble(Object base, long offset, double x) {
        if (AlignmentUtil.is8BytesAligned(offset)) {
            UNSAFE.putDouble(base, offset, x);
        } else {
            EndiannessUtil.writeDouble(EndiannessUtil.NATIVE_ACCESS, base, offset, x, AlignmentUtil.IS_PLATFORM_BIG_ENDIAN);
        }
    }

    // Object-based concurrent operations

    @Override
    public boolean getBooleanVolatile(Object base, long offset) {
        return UNSAFE.getBooleanVolatile(base, offset);
    }

    @Override
    public void putBooleanVolatile(Object base, long offset, boolean x) {
        UNSAFE.putBooleanVolatile(base, offset, x);
    }

    @Override
    public byte getByteVolatile(Object base, long offset) {
        return UNSAFE.getByteVolatile(base, offset);
    }

    @Override
    public void putByteVolatile(Object base, long offset, byte x) {
        UNSAFE.putByteVolatile(base, offset, x);
    }

    @Override
    public Object getObjectVolatile(Object base, long offset) {
        AlignmentUtil.checkReferenceAligned(offset);
        return UNSAFE.getObjectVolatile(base, offset);
    }

    @Override
    public void putObjectVolatile(Object base, long offset, Object x) {
        AlignmentUtil.checkReferenceAligned(offset);
        UNSAFE.putObjectVolatile(base, offset, x);
    }

    @Override
    public char getCharVolatile(Object base, long offset) {
        AlignmentUtil.check2BytesAligned(offset);
        return UNSAFE.getCharVolatile(base, offset);
    }

    @Override
    public void putCharVolatile(Object base, long offset, char x) {
        AlignmentUtil.check2BytesAligned(offset);
        UNSAFE.putCharVolatile(base, offset, x);
    }

    @Override
    public short getShortVolatile(Object base, long offset) {
        AlignmentUtil.check2BytesAligned(offset);
        return UNSAFE.getShortVolatile(base, offset);
    }

    @Override
    public void putShortVolatile(Object base, long offset, short x) {
        AlignmentUtil.check2BytesAligned(offset);
        UNSAFE.putShortVolatile(base, offset, x);
    }

    @Override
    public int getIntVolatile(Object base, long offset) {
        AlignmentUtil.check4BytesAligned(offset);
        return UNSAFE.getIntVolatile(base, offset);
    }

    @Override
    public void putIntVolatile(Object base, long offset, int x) {
        AlignmentUtil.check4BytesAligned(offset);
        UNSAFE.putIntVolatile(base, offset, x);
    }

    @Override
    public float getFloatVolatile(Object base, long offset) {
        AlignmentUtil.check4BytesAligned(offset);
        return UNSAFE.getFloatVolatile(base, offset);
    }

    @Override
    public void putFloatVolatile(Object base, long offset, float x) {
        AlignmentUtil.check4BytesAligned(offset);
        UNSAFE.putFloatVolatile(base, offset, x);
    }

    @Override
    public long getLongVolatile(Object base, long offset) {
        AlignmentUtil.check8BytesAligned(offset);
        return UNSAFE.getLongVolatile(base, offset);
    }

    @Override
    public void putLongVolatile(Object base, long offset, long x) {
        AlignmentUtil.check8BytesAligned(offset);
        UNSAFE.putLongVolatile(base, offset, x);
    }

    @Override
    public double getDoubleVolatile(Object base, long offset) {
        AlignmentUtil.check8BytesAligned(offset);
        return UNSAFE.getDoubleVolatile(base, offset);
    }

    @Override
    public void putDoubleVolatile(Object base, long offset, double x) {
        AlignmentUtil.check8BytesAligned(offset);
        UNSAFE.putDoubleVolatile(base, offset, x);
    }

    @Override
    public void putOrderedInt(Object base, long offset, int x) {
        AlignmentUtil.check4BytesAligned(offset);
        UNSAFE.putOrderedInt(base, offset, x);
    }

    @Override
    public void putOrderedLong(Object base, long offset, long x) {
        AlignmentUtil.check8BytesAligned(offset);
        UNSAFE.putOrderedLong(base, offset, x);
    }

    @Override
    public void putOrderedObject(Object base, long offset, Object x) {
        AlignmentUtil.checkReferenceAligned(offset);
        UNSAFE.putOrderedObject(base, offset, x);
    }

    @Override
    public boolean compareAndSwapInt(Object base, long offset, int expected, int x) {
        AlignmentUtil.check4BytesAligned(offset);
        return UNSAFE.compareAndSwapInt(base, offset, expected, x);
    }

    @Override
    public boolean compareAndSwapLong(Object base, long offset, long expected, long x) {
        AlignmentUtil.check8BytesAligned(offset);
        return UNSAFE.compareAndSwapLong(base, offset, expected, x);
    }

    @Override
    public boolean compareAndSwapObject(Object base, long offset, Object expected, Object x) {
        AlignmentUtil.checkReferenceAligned(offset);
        return UNSAFE.compareAndSwapObject(base, offset, expected, x);
    }
}

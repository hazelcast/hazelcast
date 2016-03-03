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

import static com.hazelcast.internal.memory.impl.AlignmentUtil.is2BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.is4BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.is8BytesAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.isReferenceAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.checkReferenceAligned;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.OBJECT_REFERENCE_ALIGN;

import java.nio.ByteOrder;

@SuppressWarnings({"checkstyle:magicnumber", "checkstyle:methodcount"})
public class AlignmentMemoryAccessStrategy extends UnsafeBasedMemoryAccessStrategy {
    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    @Override
    public void copyMemory(Object srcObj, long srcOffset, Object destObj, long destOffset, long bytes) {
        // TODO Should we check and handle alignment???
        super.copyMemory(srcObj, srcOffset, destObj, destOffset, bytes);
    }

    @Override
    public char getChar(Object o, long offset) {
        if (is2BytesAligned(offset)) {
            return super.getChar(o, offset);
        } else {
            return EndiannessUtility.readChar(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public char getCharVolatile(Object o, long offset) {
        if (is2BytesAligned(offset)) {
            return super.getCharVolatile(o, offset);
        } else {
            return EndiannessUtility.readCharVolatile(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public void putChar(Object o, long offset, char x) {
        if (is2BytesAligned(offset)) {
            super.putChar(o, offset, x);
        } else {
            EndiannessUtility.writeChar(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public void putCharVolatile(Object o, long offset, char x) {
        if (is2BytesAligned(offset)) {
            super.putChar(o, offset, x);
        } else {
            EndiannessUtility.writeCharVolatile(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public short getShort(Object o, long offset) {
        if (is2BytesAligned(offset)) {
            return super.getShort(o, offset);
        } else {
            return EndiannessUtility.readShort(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public short getShortVolatile(Object o, long offset) {
        if (is2BytesAligned(offset)) {
            return super.getShortVolatile(o, offset);
        } else {
            return EndiannessUtility.readShortVolatile(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public void putShort(Object o, long offset, short x) {
        if (is2BytesAligned(offset)) {
            super.putShort(o, offset, x);
        } else {
            EndiannessUtility.writeShort(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public void putShortVolatile(Object o, long offset, short x) {
        if (is2BytesAligned(offset)) {
            super.putShortVolatile(o, offset, x);
        } else {
            EndiannessUtility.writeShortVolatile(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public int getInt(Object o, long offset) {
        if (is4BytesAligned(offset)) {
            return super.getInt(o, offset);
        } else {
            return EndiannessUtility.readInt(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public int getIntVolatile(Object o, long offset) {
        if (is4BytesAligned(offset)) {
            return super.getIntVolatile(o, offset);
        } else {
            return EndiannessUtility.readIntVolatile(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public void putInt(Object o, long offset, int x) {
        if (is4BytesAligned(offset)) {
            super.putInt(o, offset, x);
        } else {
            EndiannessUtility.writeInt(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public void putIntVolatile(Object o, long offset, int x) {
        if (is4BytesAligned(offset)) {
            super.putIntVolatile(o, offset, x);
        } else {
            EndiannessUtility.writeIntVolatile(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public float getFloat(Object o, long offset) {
        if (is4BytesAligned(offset)) {
            return super.getFloat(o, offset);
        } else {
            return EndiannessUtility.readFloat(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public float getFloatVolatile(Object o, long offset) {
        if (is4BytesAligned(offset)) {
            return super.getFloatVolatile(o, offset);
        } else {
            return EndiannessUtility.readFloatVolatile(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public void putFloat(Object o, long offset, float x) {
        if (is4BytesAligned(offset)) {
            super.putFloat(o, offset, x);
        } else {
            EndiannessUtility.writeFloat(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public void putFloatVolatile(Object o, long offset, float x) {
        if (is4BytesAligned(offset)) {
            super.putFloatVolatile(o, offset, x);
        } else {
            EndiannessUtility.writeFloatVolatile(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public long getLong(Object o, long offset) {
        if (is8BytesAligned(offset)) {
            return super.getLong(o, offset);
        } else {
            return EndiannessUtility.readLong(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public long getLongVolatile(Object o, long offset) {
        if (is8BytesAligned(offset)) {
            return super.getLongVolatile(o, offset);
        } else {
            return EndiannessUtility.readLongVolatile(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public void putLong(Object o, long offset, long x) {
        if (is8BytesAligned(offset)) {
            super.putLong(o, offset, x);
        } else {
            EndiannessUtility.writeLong(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public void putLongVolatile(Object o, long offset, long x) {
        if (is8BytesAligned(offset)) {
            super.putLongVolatile(o, offset, x);
        } else {
            EndiannessUtility.writeLongVolatile(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public double getDouble(Object o, long offset) {
        if (is8BytesAligned(offset)) {
            return super.getDouble(o, offset);
        } else {
            return EndiannessUtility.readDouble(o, offset, BIG_ENDIAN);
        }
    }

    @Override
    public double getDoubleVolatile(Object o, long offset) {
        if (is8BytesAligned(offset)) {
            return super.getDoubleVolatile(o, offset);
        } else {
            return EndiannessUtility.readDoubleVolatile(o, offset, BIG_ENDIAN);
        }
    }


    @Override
    public void putDouble(Object o, long offset, double x) {
        if (is8BytesAligned(offset)) {
            super.putDouble(o, offset, x);
        } else {
            EndiannessUtility.writeDouble(o, offset, x, BIG_ENDIAN);
        }
    }

    @Override
    public void putDoubleVolatile(Object o, long offset, double x) {
        if (is8BytesAligned(offset)) {
            super.putDoubleVolatile(o, offset, x);
        } else {
            EndiannessUtility.writeDoubleVolatile(o, offset, x, BIG_ENDIAN);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public Object getObject(Object o, long offset) {
        checkReferenceAligned(offset);
        return super.getObject(o, offset);
    }

    @Override
    public Object getObjectVolatile(Object o, long offset) {
        checkReferenceAligned(offset);
        return super.getObjectVolatile(o, offset);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putObject(Object o, long offset, Object x) {
        checkReferenceAligned(offset);
        super.putObject(o, offset, x);
    }

    @Override
    public void putObjectVolatile(Object o, long offset, Object x) {
        checkReferenceAligned(offset);
        super.putObjectVolatile(o, offset, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public boolean compareAndSwapInt(Object o, long offset, int expected, int x) {
        if (is4BytesAligned(offset)) {
            return super.compareAndSwapInt(o, offset, expected, x);
        } else {
            throw new IllegalArgumentException("Unaligned memory accesses are not supported for CAS operations. "
                    + "Offset must be 4-bytes aligned for integer typed CAS, but it is " + offset);
        }
    }

    @Override
    public boolean compareAndSwapLong(Object o, long offset, long expected, long x) {
        if (is4BytesAligned(offset)) {
            return super.compareAndSwapLong(o, offset, expected, x);
        } else {
            throw new IllegalArgumentException("Unaligned memory accesses are not supported for CAS operations. "
                    + "Offset must be 8-bytes aligned for long typed CAS, but it is " + offset);
        }
    }

    @Override
    public boolean compareAndSwapObject(Object o, long offset, Object expected, Object x) {
        if (isReferenceAligned(offset)) {
            return super.compareAndSwapObject(o, offset, expected, x);
        } else {
            throw new IllegalArgumentException("Unaligned memory accesses are not supported for CAS operations. "
                    + "Offset must be " + OBJECT_REFERENCE_ALIGN + "-bytes "
                    + "aligned for object reference typed CAS, but it is " + offset);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putOrderedInt(Object o, long offset, int x) {
        if (is4BytesAligned(offset)) {
            super.putOrderedInt(o, offset, x);
        } else {
            throw new IllegalArgumentException("Unaligned memory accesses are not supported for ordered writes. "
                    + "Offset must be 4-bytes aligned for integer typed ordered write, but it is " + offset);
        }
    }

    @Override
    public void putOrderedLong(Object o, long offset, long x) {
        if (is8BytesAligned(offset)) {
            super.putOrderedLong(o, offset, x);
        } else {
            throw new IllegalArgumentException("Unaligned memory accesses are not supported for ordered writes. "
                    + "Offset must be 8-bytes aligned for long typed ordered write, but it is " + offset);
        }
    }

    @Override
    public void putOrderedObject(Object o, long offset, Object x) {
        if (isReferenceAligned(offset)) {
            super.putOrderedObject(o, offset, x);
        } else {
            throw new IllegalArgumentException("Unaligned memory accesses are not supported for CAS operations. "
                    + "Offset must be " + OBJECT_REFERENCE_ALIGN + "-bytes "
                    + "aligned for object reference typed ordered writes, but it is " + offset);
        }
    }
}

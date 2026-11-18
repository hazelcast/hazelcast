/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import static com.hazelcast.internal.memory.impl.UnsafeUtil.UNSAFE_AVAILABLE;

/**
 * Diagnostic variant of {@link StandardMemoryAccessor} allowing to detect invalid
 * unaligned memory access on CPUs that do not fail for them. Checks also atomic operations
 * which might succeed in some cases even if unaligned, but are forbidden.
 * When used in tests it can provide more meaningful error than JVM crash.
 * <p>
 * This is useful for:
 * <ol>
 *     <li>Detecting invalid unaligned concurrent operations early</li>
 *     <li>Checking alignment-sensitive code on CPUs that allow unaligned access,
 *         improving support for platforms that require alignment.</li>
 *     <li>Ensuring that there is no undesirable unaligned access in performance-critical code</li>
 * </ol>
 */
@SuppressWarnings("checkstyle:methodcount")
public final class DiagnosticStandardMemoryAccessor extends StandardMemoryAccessor {
    public static final DiagnosticStandardMemoryAccessor INSTANCE =
            UNSAFE_AVAILABLE ? new DiagnosticStandardMemoryAccessor() : null;

    private DiagnosticStandardMemoryAccessor() {
        if (!UNSAFE_AVAILABLE) {
            throw new IllegalStateException(getClass().getName() + " can only be used only when Unsafe is available!");
        }
    }

    // Address-based access

    @Override
    public char getChar(long address) {
        trapUnaligned2Bytes(address);
        return super.getChar(address);
    }

    @Override
    public void putChar(long address, char x) {
        trapUnaligned2Bytes(address);
        super.putChar(address, x);
    }

    @Override
    public short getShort(long address) {
        trapUnaligned2Bytes(address);
        return super.getShort(address);
    }

    @Override
    public void putShort(long address, short x) {
        trapUnaligned2Bytes(address);
        super.putShort(address, x);
    }

    @Override
    public int getInt(long address) {
        trapUnaligned4Bytes(address);
        return super.getInt(address);
    }

    @Override
    public void putInt(long address, int x) {
        trapUnaligned4Bytes(address);
        super.putInt(address, x);
    }

    @Override
    public float getFloat(long address) {
        trapUnaligned4Bytes(address);
        return super.getFloat(address);
    }

    @Override
    public void putFloat(long address, float x) {
        trapUnaligned4Bytes(address);
        super.putFloat(address, x);
    }

    @Override
    public long getLong(long address) {
        trapUnaligned8Bytes(address);
        return super.getLong(address);
    }

    @Override
    public void putLong(long address, long x) {
        trapUnaligned8Bytes(address);
        super.putLong(address, x);
    }

    @Override
    public double getDouble(long address) {
        trapUnaligned8Bytes(address);
        return super.getDouble(address);
    }

    @Override
    public void putDouble(long address, double x) {
        trapUnaligned8Bytes(address);
        super.putDouble(address, x);
    }


    // Address-based concurrent operations

    @Override
    public char getCharVolatile(long address) {
        AlignmentUtil.check2BytesAligned(address);
        return super.getCharVolatile(null, address);
    }

    @Override
    public void putCharVolatile(long address, char x) {
        AlignmentUtil.check2BytesAligned(address);
        super.putCharVolatile(null, address, x);
    }

    @Override
    public short getShortVolatile(long address) {
        AlignmentUtil.check2BytesAligned(address);
        return super.getShortVolatile(null, address);
    }

    @Override
    public void putShortVolatile(long address, short x) {
        AlignmentUtil.check2BytesAligned(address);
        super.putShortVolatile(null, address, x);
    }

    @Override
    public int getIntVolatile(long address) {
        AlignmentUtil.check4BytesAligned(address);
        return super.getIntVolatile(null, address);
    }

    @Override
    public void putIntVolatile(long address, int x) {
        AlignmentUtil.check4BytesAligned(address);
        super.putIntVolatile(null, address, x);
    }

    @Override
    public float getFloatVolatile(long address) {
        AlignmentUtil.check4BytesAligned(address);
        return super.getFloatVolatile(null, address);
    }

    @Override
    public void putFloatVolatile(long address, float x) {
        AlignmentUtil.check4BytesAligned(address);
        super.putFloatVolatile(null, address, x);
    }

    @Override
    public long getLongVolatile(long address) {
        AlignmentUtil.check8BytesAligned(address);
        return super.getLongVolatile(null, address);
    }

    @Override
    public void putLongVolatile(long address, long x) {
        AlignmentUtil.check8BytesAligned(address);
        super.putLongVolatile(null, address, x);
    }

    @Override
    public double getDoubleVolatile(long address) {
        AlignmentUtil.check8BytesAligned(address);
        return super.getDoubleVolatile(null, address);
    }

    @Override
    public void putDoubleVolatile(long address, double x) {
        AlignmentUtil.check8BytesAligned(address);
        super.putDoubleVolatile(null, address, x);
    }

    @Override
    public void putOrderedInt(long address, int x) {
        AlignmentUtil.check4BytesAligned(address);
        super.putOrderedInt(null, address, x);
    }

    @Override
    public void putOrderedLong(long address, long x) {
        AlignmentUtil.check8BytesAligned(address);
        super.putOrderedLong(null, address, x);
    }

    @Override
    public void putOrderedObject(long address, Object x) {
        AlignmentUtil.checkReferenceAligned(address);
        super.putOrderedObject(null, address, x);
    }

    @Override
    public boolean compareAndSwapInt(long address, int expected, int x) {
        AlignmentUtil.check4BytesAligned(address);
        return super.compareAndSwapInt(null, address, expected, x);
    }

    @Override
    public boolean compareAndSwapLong(long address, long expected, long x) {
        AlignmentUtil.check8BytesAligned(address);
        return super.compareAndSwapLong(null, address, expected, x);
    }

    @Override
    public boolean compareAndSwapObject(long address, Object expected, Object x) {
        AlignmentUtil.checkReferenceAligned(address);
        return super.compareAndSwapObject(null, address, expected, x);
    }

    // Object-based access
    // Objects on heap are 8-byte aligned
    @Override
    public Object getObject(Object base, long offset) {
        AlignmentUtil.checkReferenceAligned(offset);
        return super.getObject(base, offset);
    }

    @Override
    public void putObject(Object base, long offset, Object x) {
        AlignmentUtil.checkReferenceAligned(offset);
        super.putObject(base, offset, x);
    }

    @Override
    public char getChar(Object base, long offset) {
        trapUnaligned2Bytes(offset);
        return super.getChar(base, offset);
    }

    @Override
    public void putChar(Object base, long offset, char x) {
        trapUnaligned2Bytes(offset);
        super.putChar(base, offset, x);
    }

    @Override
    public short getShort(Object base, long offset) {
        trapUnaligned2Bytes(offset);
        return super.getShort(base, offset);
    }

    @Override
    public void putShort(Object base, long offset, short x) {
        trapUnaligned2Bytes(offset);
        super.putShort(base, offset, x);
    }

    @Override
    public int getInt(Object base, long offset) {
        trapUnaligned4Bytes(offset);
        return super.getInt(base, offset);
    }

    @Override
    public void putInt(Object base, long offset, int x) {
        trapUnaligned4Bytes(offset);
        super.putInt(base, offset, x);
    }

    @Override
    public float getFloat(Object base, long offset) {
        trapUnaligned4Bytes(offset);
        return super.getFloat(base, offset);
    }

    @Override
    public void putFloat(Object base, long offset, float x) {
        trapUnaligned4Bytes(offset);
        super.putFloat(base, offset, x);
    }

    @Override
    public long getLong(Object base, long offset) {
        trapUnaligned8Bytes(offset);
        return super.getLong(base, offset);
    }

    @Override
    public void putLong(Object base, long offset, long x) {
        trapUnaligned8Bytes(offset);
        super.putLong(base, offset, x);
    }

    @Override
    public double getDouble(Object base, long offset) {
        trapUnaligned8Bytes(offset);
        return super.getDouble(base, offset);
    }

    @Override
    public void putDouble(Object base, long offset, double x) {
        trapUnaligned8Bytes(offset);
        super.putDouble(base, offset, x);
    }


    // Object-based concurrent operations

    @Override
    public Object getObjectVolatile(Object base, long offset) {
        AlignmentUtil.checkReferenceAligned(offset);
        return super.getObjectVolatile(base, offset);
    }

    @Override
    public void putObjectVolatile(Object base, long offset, Object x) {
        AlignmentUtil.checkReferenceAligned(offset);
        super.putObjectVolatile(base, offset, x);
    }

    @Override
    public char getCharVolatile(Object base, long offset) {
        AlignmentUtil.check2BytesAligned(offset);
        return super.getCharVolatile(base, offset);
    }

    @Override
    public void putCharVolatile(Object base, long offset, char x) {
        AlignmentUtil.check2BytesAligned(offset);
        super.putCharVolatile(base, offset, x);
    }

    @Override
    public short getShortVolatile(Object base, long offset) {
        AlignmentUtil.check2BytesAligned(offset);
        return super.getShortVolatile(base, offset);
    }

    @Override
    public void putShortVolatile(Object base, long offset, short x) {
        AlignmentUtil.check2BytesAligned(offset);
        super.putShortVolatile(base, offset, x);
    }

    @Override
    public int getIntVolatile(Object base, long offset) {
        AlignmentUtil.check4BytesAligned(offset);
        return super.getIntVolatile(base, offset);
    }

    @Override
    public void putIntVolatile(Object base, long offset, int x) {
        AlignmentUtil.check4BytesAligned(offset);
        super.putIntVolatile(base, offset, x);
    }

    @Override
    public float getFloatVolatile(Object base, long offset) {
        AlignmentUtil.check4BytesAligned(offset);
        return super.getFloatVolatile(base, offset);
    }

    @Override
    public void putFloatVolatile(Object base, long offset, float x) {
        AlignmentUtil.check4BytesAligned(offset);
        super.putFloatVolatile(base, offset, x);
    }

    @Override
    public long getLongVolatile(Object base, long offset) {
        AlignmentUtil.check8BytesAligned(offset);
        return super.getLongVolatile(base, offset);
    }

    @Override
    public void putLongVolatile(Object base, long offset, long x) {
        AlignmentUtil.check8BytesAligned(offset);
        super.putLongVolatile(base, offset, x);
    }

    @Override
    public double getDoubleVolatile(Object base, long offset) {
        AlignmentUtil.check8BytesAligned(offset);
        return super.getDoubleVolatile(base, offset);
    }

    @Override
    public void putDoubleVolatile(Object base, long offset, double x) {
        AlignmentUtil.check8BytesAligned(offset);
        super.putDoubleVolatile(base, offset, x);
    }

    @Override
    public void putOrderedInt(Object base, long offset, int x) {
        AlignmentUtil.check4BytesAligned(offset);
        super.putOrderedInt(base, offset, x);
    }

    @Override
    public void putOrderedLong(Object base, long offset, long x) {
        AlignmentUtil.check8BytesAligned(offset);
        super.putOrderedLong(base, offset, x);
    }

    @Override
    public void putOrderedObject(Object base, long offset, Object x) {
        AlignmentUtil.checkReferenceAligned(offset);
        super.putOrderedObject(base, offset, x);
    }

    @Override
    public boolean compareAndSwapInt(Object base, long offset, int expected, int x) {
        AlignmentUtil.check4BytesAligned(offset);
        return super.compareAndSwapInt(base, offset, expected, x);
    }

    @Override
    public boolean compareAndSwapLong(Object base, long offset, long expected, long x) {
        AlignmentUtil.check8BytesAligned(offset);
        return super.compareAndSwapLong(base, offset, expected, x);
    }

    @Override
    public boolean compareAndSwapObject(Object base, long offset, Object expected, Object x) {
        AlignmentUtil.checkReferenceAligned(offset);
        return super.compareAndSwapObject(base, offset, expected, x);
    }

    private static void trapUnaligned2Bytes(long address) {
        if (!is2BytesAligned(address)) {
            throw new IllegalArgumentException("Memory access must be aligned, but the address used was " + address);
        }
    }

    private static void trapUnaligned4Bytes(long address) {
        if (!is4BytesAligned(address)) {
            throw new IllegalArgumentException("Memory access must be aligned, but the address used was " + address);
        }
    }

    private static void trapUnaligned8Bytes(long address) {
        if (!is8BytesAligned(address)) {
            throw new IllegalArgumentException("Memory access must be aligned, but the address used was " + address);
        }
    }
}

package com.hazelcast.internal.memory;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

public class UnsafeAllocator implements MemoryAllocator {
    private final static Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    public static final UnsafeAllocator INSTANCE = new UnsafeAllocator();

    @Override
    public long allocate(long size) {
        try {
            return UNSAFE.allocateMemory(size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        try {
            return UNSAFE.reallocateMemory(address, newSize);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public void free(long address, long size) {
        UNSAFE.freeMemory(address);
    }

    @Override
    public void dispose() {

    }
}

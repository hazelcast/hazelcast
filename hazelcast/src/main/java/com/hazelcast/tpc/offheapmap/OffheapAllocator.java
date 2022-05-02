package com.hazelcast.tpc.offheapmap;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

public final class OffheapAllocator implements Allocator {

    private final Unsafe unsafe = UnsafeUtil.UNSAFE;

    @Override
    public long allocate(long size) {
        return unsafe.allocateMemory(size);
    }

    @Override
    public long callocate(long size) {
        long address = allocate(size);
        unsafe.setMemory(address, size, (byte) 0);
        return address;
    }

    @Override
    public long reallocate(long address, long bytes){
        return unsafe.reallocateMemory(address, bytes);
    }

    @Override
    public void free(long address) {
        unsafe.freeMemory(address);
    }
}

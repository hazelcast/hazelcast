package com.hazelcast.tpc;

public interface Allocator {

    long allocate(long size);

    long callocate(long size);

    long reallocate(long address, long bytes);

    void free(long address);
}

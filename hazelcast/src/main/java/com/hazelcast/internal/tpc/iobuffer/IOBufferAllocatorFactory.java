package com.hazelcast.internal.tpc.iobuffer;

public abstract class IOBufferAllocatorFactory {
    private IOBufferAllocatorFactory() {
    }

    public static IOBufferAllocator<? extends IOBuffer> createThreadLocal() {
        return new ThreadLocalIOBufferAllocator();
    }

    public static IOBufferAllocator<? extends IOBuffer> createConcurrentAllocator() {
        // TODO
        return null;
    }
}

package com.hazelcast.internal.tpc.iobuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

class ThreadLocalIOBufferAllocator implements IOBufferAllocator<ThreadLocalIOBuffer> {
    static final int DEFAULT_SIZE = 4096;
    static final int INITIAL_POOL_SIZE = 4096;
    static final int BUFFER_SIZE = 16384;

    ByteBuffer[] byteBufferPool = new ByteBuffer[INITIAL_POOL_SIZE];
    ThreadLocalIOBuffer[] ioBufferPool = new ThreadLocalIOBuffer[INITIAL_POOL_SIZE];

    int byteBufferPoolPos;
    int ioBufferPoolPos;

    @Override
    public ThreadLocalIOBuffer allocate() {
        return allocate(DEFAULT_SIZE);
    }

    @Override
    public ThreadLocalIOBuffer allocate(int minSize) {
        return getNextIOBuffer(minSize);
    }

    @Override
    public void free(ThreadLocalIOBuffer ioBuffer) {
        for (int i = ioBuffer.chunkToRelease; i < ioBuffer.chunks.length; i++) {
            ByteBuffer chunk = ioBuffer.chunks[i];
            reclaim(chunk);
        }
        reclaim(ioBuffer);
    }

    @Override
    public void free(ByteBuffer chunk) {
        reclaim(chunk);
    }

    ByteBuffer getNextByteBuffer() {
        if (byteBufferPoolPos == 0) {
            return ByteBuffer.allocateDirect(BUFFER_SIZE);
        }
        return byteBufferPool[--byteBufferPoolPos];
    }

    private ThreadLocalIOBuffer getNextIOBuffer(int minSize) {
        if (ioBufferPoolPos == 0) {
            return new ThreadLocalIOBuffer(this, minSize);
        }
        ThreadLocalIOBuffer buffer = ioBufferPool[--ioBufferPoolPos];
        buffer.reset(minSize);
        return buffer;
    }

    private void reclaim(ByteBuffer byteBuffer) {
        ensureRemainingByteBuffer();
        byteBufferPool[byteBufferPoolPos++] = byteBuffer;
    }

    private void reclaim(ThreadLocalIOBuffer ioBuffer) {
        ensureRemainingIoBuffer();
        ioBufferPool[ioBufferPoolPos++] = ioBuffer;
    }

    /**
     * TODO: creates litter during warmup, can be replaced with object array pool.
     */
    private void ensureRemainingByteBuffer() {
        if (byteBufferPoolPos == byteBufferPool.length) {
            byteBufferPool = Arrays.copyOf(byteBufferPool, byteBufferPool.length * 2);
        }
    }

    private void ensureRemainingIoBuffer() {
        if (ioBufferPoolPos == ioBufferPool.length) {
            ioBufferPool = Arrays.copyOf(ioBufferPool, ioBufferPool.length * 2);
        }
    }
}

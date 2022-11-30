package com.hazelcast.internal.tpc.iobuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

class ThreadLocalIOBufferAllocator implements IOBufferAllocator<ThreadLocalIOBuffer> {
    static final int INITIAL_POOL_SIZE = 4096;
    static final int BUFFER_SIZE = 16384;

    ByteBuffer[] byteBufferPool;
    ThreadLocalIOBuffer[] ioBufferPool;

    int byteBufferPoolPos;
    int ioBufferPoolPos;

    private final boolean growing;
    private final ConcurrentIOBufferAllocator concurrentAllocator;

    ThreadLocalIOBufferAllocator(boolean growing, int initialPoolSize, ConcurrentIOBufferAllocator concurrentAllocator) {
        this.growing = growing;
        this.byteBufferPool = new ByteBuffer[initialPoolSize];
        this.ioBufferPool = new ThreadLocalIOBuffer[initialPoolSize];
        this.concurrentAllocator = concurrentAllocator;
    }

    @Override
    public ThreadLocalIOBuffer allocate() {
        return allocate(DEFAULT_IO_BUFFER_SIZE);
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

    void freeExternalWithoutByteBuffers(ThreadLocalIOBuffer ioBuffer) {
        reclaim(ioBuffer);
        ioBuffer.overtakenBy(this);
    }

    boolean hasSpaceForIOBuffer() {
        return growing || ioBufferPoolPos < ioBufferPool.length;
    }

    boolean hasSpaceForByteBuffer() {
        return growing || ioBufferPoolPos < byteBufferPool.length;
    }

    @Override
    public void free(ByteBuffer chunk) {
        reclaim(chunk);
    }

    ByteBuffer getNextByteBuffer() {
        if (byteBufferPoolPos == 0) {
            return ByteBuffer.allocateDirect(BUFFER_SIZE);
        }
        ByteBuffer byteBuffer = byteBufferPool[--byteBufferPoolPos];
        byteBuffer.clear();
        return byteBuffer;
    }

    private ThreadLocalIOBuffer getNextIOBuffer(int minSize) {
        if (ioBufferPoolPos == 0) {
            return new ThreadLocalIOBuffer(this, minSize, concurrentAllocator);
        }
        ThreadLocalIOBuffer buffer = ioBufferPool[--ioBufferPoolPos];
        buffer.reset(minSize);
        return buffer;
    }

    private void reclaim(ByteBuffer byteBuffer) {
        if (!ensureRemainingByteBuffer()) {
            return;
        }
        byteBufferPool[byteBufferPoolPos++] = byteBuffer;
    }

    private void reclaim(ThreadLocalIOBuffer ioBuffer) {
        if (!ensureRemainingIoBuffer()) {
            return;
        }
        ioBufferPool[ioBufferPoolPos++] = ioBuffer;
    }

    private boolean ensureRemainingByteBuffer() {
        if (byteBufferPoolPos == byteBufferPool.length) {
            if (!growing) {
                return false;
            }
            byteBufferPool = Arrays.copyOf(byteBufferPool, byteBufferPool.length * 2);
        }
        return true;
    }

    private boolean ensureRemainingIoBuffer() {
        if (ioBufferPoolPos == ioBufferPool.length) {
            if (!growing) {
                return false;
            }
            ioBufferPool = Arrays.copyOf(ioBufferPool, ioBufferPool.length * 2);
        }
        return true;
    }
}

package com.hazelcast.internal.tpc.iobuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

class TpcIOBufferAllocator implements IOBufferAllocator {
    private static final int DEFAULT_SIZE = 4096;
    private static final int INITIAL_POOL_SIZE = 4096;
    private static final int BUFFER_SIZE = 16384;

    private ByteBuffer[] bufferPool = new ByteBuffer[INITIAL_POOL_SIZE];
    private int bufferPoolPos;

    @Override
    public IOBuffer allocate() {
        return allocate(DEFAULT_SIZE);
    }

    @Override
    public IOBuffer allocate(int minSize) {
        return null;
    }

    @Override
    public void free(IOBuffer buf) {

    }

    private ByteBuffer getNextBuffer() {
        if (bufferPoolPos == 0) {
            return ByteBuffer.allocateDirect(BUFFER_SIZE);
        }
        return bufferPool[--bufferPoolPos];
    }

    private void reclaim(ByteBuffer byteBuffer) {
        makeRoom();
        bufferPool[bufferPoolPos++] = byteBuffer;
    }

    /**
     * TODO: creates litter during warmup, can be replaced with object array pool.
     */
    private void makeRoom() {
        if (bufferPoolPos == bufferPool.length) {
            bufferPool = Arrays.copyOf(bufferPool, bufferPool.length * 2);
        }
    }
}

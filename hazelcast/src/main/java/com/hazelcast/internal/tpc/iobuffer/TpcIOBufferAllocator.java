package com.hazelcast.internal.tpc.iobuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

class TpcIOBufferAllocator implements IOBufferAllocator<TpcIOBuffer> {
    static final int DEFAULT_SIZE = 4096;
    static final int INITIAL_POOL_SIZE = 4096;
    static final int BUFFER_SIZE = 16384;

    ByteBuffer[] bufferPool = new ByteBuffer[INITIAL_POOL_SIZE];
    private int bufferPoolPos;

    @Override
    public TpcIOBuffer allocate() {
        return allocate(DEFAULT_SIZE);
    }

    @Override
    public TpcIOBuffer allocate(int minSize) {
        return new TpcIOBuffer(this, minSize);
    }

    @Override
    public void free(TpcIOBuffer buf) {
        for (int i = 0; i < buf.chunks.length; i++) {
            ByteBuffer chunk = buf.chunks[i];
            reclaim(chunk);
        }
    }

    ByteBuffer getNextBuffer() {
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

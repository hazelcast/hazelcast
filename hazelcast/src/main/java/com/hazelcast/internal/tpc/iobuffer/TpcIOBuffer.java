package com.hazelcast.internal.tpc.iobuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

class TpcIOBuffer {
    private ByteBuffer[] chunks;
    private int chunksPos;

    TpcIOBuffer(int minSize) {
        chunks = new ByteBuffer[minSize];
    }

    void addChunk(ByteBuffer chunk) {
        makeRoom();
        chunks[chunksPos++] = chunk;
    }

    /**
     * TODO: creates litter during warmup, can be replaced with object array pool.
     */
    private void makeRoom() {
        if (chunksPos == chunks.length) {
            chunks = Arrays.copyOf(chunks, chunks.length * 2);
        }
    }
}

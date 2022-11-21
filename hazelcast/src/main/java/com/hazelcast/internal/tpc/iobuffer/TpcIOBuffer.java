package com.hazelcast.internal.tpc.iobuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.hazelcast.internal.tpc.iobuffer.TpcIOBufferAllocator.BUFFER_SIZE;

class TpcIOBuffer implements IOBuffer {
    private final TpcIOBufferAllocator allocator;

    private ByteBuffer[] chunks;

    /**
     * Position of a last chunk in.
     */
    private int chunksPos;

    /**
     * Position of a last byte in a buffer.
     */
    private int pos;

    private int totalCapacity;

    TpcIOBuffer(TpcIOBufferAllocator allocator, int minSize) {
        this.allocator = allocator;
        this.chunks = new ByteBuffer[minSize];
    }

    @Override
    public void release() {
        allocator.free(this);
    }

    @Override
    public int position() {
        return pos;
    }


    @Override
    public byte getByte(int pos) {
        int chunk = pos / BUFFER_SIZE;
        int posInChunk = pos % BUFFER_SIZE;
        return chunks[chunk].get(posInChunk);
    }


    void addChunk(ByteBuffer chunk) {
        makeRoom();
        chunks[chunksPos++] = chunk;
        totalCapacity += BUFFER_SIZE;
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

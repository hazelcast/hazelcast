/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.tpc.iobuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.hazelcast.internal.tpc.iobuffer.ThreadLocalIOBufferAllocator.BUFFER_SIZE;
import static com.hazelcast.internal.tpc.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.tpc.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.tpc.nio.Bits.SHORT_SIZE_IN_BYTES;

class ThreadLocalIOBuffer implements IOBuffer {
    ByteBuffer[] chunks;

    private final ConcurrentIOBufferAllocator superAllocator;
    private ThreadLocalIOBufferAllocator allocator;

    /**
     * Position of a last byte in a buffer.
     */
    private int pos;

    private int limit;

    /*&
     * Position of a last chunk in.
     */
    private int chunksPos;

    private int chunkToRelease;

    ThreadLocalIOBuffer(ThreadLocalIOBufferAllocator allocator, int minSize, ConcurrentIOBufferAllocator concurrentAllocator) {
        this.allocator = allocator;
        this.superAllocator = concurrentAllocator;
        this.chunks = new ByteBuffer[((minSize - 1) / BUFFER_SIZE) + 1];

        for (int i = 0; i < chunks.length; i++) {
            addChunk(allocator.getNextByteBuffer());
        }
    }

    void reset(int minSize) {
        this.chunksPos = 0;
        this.pos = 0;
        this.limit = 0;
        this.chunkToRelease = 0;

        for (int i = 0; i < ((minSize - 1) / BUFFER_SIZE) + 1; i++) {
            addChunk(allocator.getNextByteBuffer());
        }
    }

    void overtakenBy(ThreadLocalIOBufferAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void release() {
        if (superAllocator == null) {
            allocator.free(this);
            return;
        }
        superAllocator.free(this);
    }

    @Override
    public void releaseNextChunk(ByteBuffer chunk) {
        assert chunk == chunks[chunkToRelease];

        if (superAllocator == null) {
            allocator.free(chunks[chunkToRelease++]);
            return;
        }
        superAllocator.free(chunks[chunkToRelease++]);
    }

    @Override
    public int position() {
        return pos;
    }

    @Override
    public void clear() {
        for (int i = 0; i < chunksPos; i++) {
            chunks[i].clear();
        }
        pos = 0;
    }

    @Override
    public void flip() {
        limit = pos;
        pos = 0;
        for (int i = 0; i < chunksPos; i++) {
            chunks[i].flip();
        }
    }

    @Override
    public byte getByte(int pos) {
        int chunk = pos / BUFFER_SIZE;
        int posInChunk = pos % BUFFER_SIZE;
        return chunks[chunk].get(posInChunk);
    }

    @Override
    public void writeByte(byte src) {
        ensureRemaining(1);
        writeByteUnsafe(src);
    }

    private void writeByteUnsafe(byte src) {
        int chunk = pos / BUFFER_SIZE;
        chunks[chunk].put(src);
        pos++;
    }

    @Override
    public void writeBytes(byte[] src) {
        ensureRemaining(src.length);
        int arrayPos = 0;
        int chunk = pos / BUFFER_SIZE;
        int toWriteRemaining = src.length - arrayPos;
        while (toWriteRemaining > 0) {
            int toWriteInCurrentChunk = Math.min(chunks[chunk].remaining(), toWriteRemaining);
            chunks[chunk++].put(src, arrayPos, toWriteInCurrentChunk);
            arrayPos += toWriteInCurrentChunk;
            toWriteRemaining -= toWriteInCurrentChunk;
            pos += toWriteInCurrentChunk;
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void writeShortL(short v) {
        ensureRemaining(SHORT_SIZE_IN_BYTES);
        writeByteUnsafe((byte) ((v) & 0xFF));
        writeByteUnsafe((byte) ((v >>> 8) & 0xFF));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public int getInt(int index) {
        int firstChunk = index / BUFFER_SIZE;
        int lastChunk = (index + INT_SIZE_IN_BYTES - 1) / BUFFER_SIZE;
        if (firstChunk == lastChunk) {
            int ret = chunks[firstChunk].getInt(index % BUFFER_SIZE);
            return ret;
        }
        int result = 0;
        for (int i = 0; i < INT_SIZE_IN_BYTES; i++) {
            result = result << 8;
            byte readByte = chunks[(index + i) / BUFFER_SIZE].get((index + i) % BUFFER_SIZE);
            result |= readByte & 0xFF;
        }
        return result;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void writeInt(int value) {
        if (BUFFER_SIZE - (pos % BUFFER_SIZE) >= INT_SIZE_IN_BYTES) {
            int chunk = pos / BUFFER_SIZE;
            if (chunk == chunksPos) {
                ensureRemaining(INT_SIZE_IN_BYTES);
            }
            chunks[chunk].putInt(value);
            pos += INT_SIZE_IN_BYTES;
            return;
        }
        ensureRemaining(INT_SIZE_IN_BYTES);
        writeByteUnsafe((byte) ((value >>> 24) & 0xFF));
        writeByteUnsafe((byte) ((value >>> 16) & 0xFF));
        writeByteUnsafe((byte) ((value >>> 8) & 0xFF));
        writeByteUnsafe((byte) (value & 0xFF));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void writeIntL(int value) {
        ensureRemaining(INT_SIZE_IN_BYTES);
        writeByteUnsafe((byte) (value & 0xFF));
        writeByteUnsafe((byte) ((value >>> 8) & 0xFF));
        writeByteUnsafe((byte) ((value >>> 16) & 0xFF));
        writeByteUnsafe((byte) ((value >>> 24) & 0xFF));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void writeLong(long value) {
        if (BUFFER_SIZE - (pos % BUFFER_SIZE) >= LONG_SIZE_IN_BYTES) {
            chunks[pos / BUFFER_SIZE].putLong(value);
            pos += LONG_SIZE_IN_BYTES;
            return;
        }

        ensureRemaining(LONG_SIZE_IN_BYTES);
        writeByteUnsafe((byte) ((value >>> 56) & 0xFF));
        writeByteUnsafe((byte) ((value >>> 48) & 0xFF));
        writeByteUnsafe((byte) ((value >>> 40) & 0xFF));
        writeByteUnsafe((byte) ((value >>> 32) & 0xFF));
        writeByteUnsafe((byte) ((value >>> 24) & 0xFF));
        writeByteUnsafe((byte) ((value >>> 16) & 0xFF));
        writeByteUnsafe((byte) ((value >>> 8) & 0xFF));
        writeByteUnsafe((byte) (value & 0xFF));
    }

    @Override
    public void write(ByteBuffer src) {
        write(src, src.remaining());
    }

    @Override
    public void write(ByteBuffer src, int count) {
        ensureRemaining(count);
        while (count > 0) {
            int chunk = pos / BUFFER_SIZE;
            int currentChunkCapacity = chunks[chunk].remaining();
            if (currentChunkCapacity >= count) {
                chunks[chunk].put(src);
                pos += count;
                count = 0;
            } else {
                int limit = src.limit();
                src.limit(src.position() + currentChunkCapacity);
                chunks[chunk].put(src);
                src.limit(limit);
                pos += currentChunkCapacity;
                count -= currentChunkCapacity;
            }
        }
    }

    @Override
    public int remaining() {
        return limit - pos;
    }

    @Override
    public ByteBuffer[] getChunks() {
        return chunks;
    }

    public int chunksPos() {
        return chunksPos;
    }

    public int chunkToRelease() {
        return chunkToRelease;
    }

    @Override
    public boolean hasRemainingChunks() {
        return chunkToRelease < chunks.length;
    }

    private void ensureRemaining(int length) {
        while (remaining() < length) {
            addChunk(allocator.getNextByteBuffer());
        }
    }

    void addChunk(ByteBuffer chunk) {
        ensureRemainingForNewChunk();
        chunks[chunksPos++] = chunk;
        limit += BUFFER_SIZE;
    }

    private void ensureRemainingForNewChunk() {
        if (chunksPos == chunks.length) {
            chunks = Arrays.copyOf(chunks, chunks.length * 2);
        }
    }
}

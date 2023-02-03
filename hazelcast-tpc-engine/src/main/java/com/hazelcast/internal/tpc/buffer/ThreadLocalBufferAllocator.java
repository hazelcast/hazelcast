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

package com.hazelcast.internal.tpc.buffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ThreadLocalBufferAllocator implements BufferAllocator<ThreadLocalBuffer> {
    static final int INITIAL_POOL_SIZE = 4096;
    static final int BUFFER_SIZE = 16384;

    ByteBuffer[] byteBufferPool;
    ThreadLocalBuffer[] bufferPool;

    int byteBufferPoolPos;
    int ioBufferPoolPos;

    private final boolean growing;
    private final ConcurrentBufferAllocator concurrentAllocator;

    ThreadLocalBufferAllocator(boolean growing, int initialPoolSize, ConcurrentBufferAllocator concurrentAllocator) {
        this.growing = growing;
        this.byteBufferPool = new ByteBuffer[initialPoolSize];
        this.bufferPool = new ThreadLocalBuffer[initialPoolSize];
        this.concurrentAllocator = concurrentAllocator;
    }

    @Override
    public ThreadLocalBuffer allocate(int minSize) {
        return getNextBuffer(minSize);
    }

    @Override
    public void free(ThreadLocalBuffer buffer) {
        for (int i = buffer.chunkToRelease(); i < buffer.chunksPos(); i++) {
            ByteBuffer chunk = buffer.chunks[i];
            reclaim(chunk);
        }
        reclaim(buffer);
    }

    void freeExternalWithoutByteBuffers(ThreadLocalBuffer buffer) {
        reclaim(buffer);
        buffer.overtakenBy(this);
    }

    boolean hasSpaceForIOBuffer() {
        return growing || ioBufferPoolPos < bufferPool.length;
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

    private ThreadLocalBuffer getNextBuffer(int minSize) {
        if (ioBufferPoolPos == 0) {
            return new ThreadLocalBuffer(this, minSize, concurrentAllocator);
        }
        ThreadLocalBuffer buffer = bufferPool[--ioBufferPoolPos];
        buffer.reset(minSize);
        return buffer;
    }

    private void reclaim(ByteBuffer byteBuffer) {
        if (!ensureRemainingByteBuffer()) {
            return;
        }
        byteBufferPool[byteBufferPoolPos++] = byteBuffer;
    }

    private void reclaim(ThreadLocalBuffer ioBuffer) {
        if (!ensureRemainingBuffer()) {
            return;
        }
        bufferPool[ioBufferPoolPos++] = ioBuffer;
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

    private boolean ensureRemainingBuffer() {
        if (ioBufferPoolPos == bufferPool.length) {
            if (!growing) {
                return false;
            }
            bufferPool = Arrays.copyOf(bufferPool, bufferPool.length * 2);
        }
        return true;
    }
}

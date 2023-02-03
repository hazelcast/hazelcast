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

import org.jctools.queues.MpmcArrayQueue;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.tpc.buffer.ThreadLocalBufferAllocator.INITIAL_POOL_SIZE;

public class ConcurrentBufferAllocator implements BufferAllocator<ThreadLocalBuffer> {
    private static final ThreadLocal<ThreadLocalBufferAllocator> THREAD_LOCAL_IO_BUFFER_ALLOCATORS
            = new ThreadLocal<>();

    private static final MpmcArrayQueue<ThreadLocalBuffer> FREED_IO_BUFFERS = new MpmcArrayQueue<>(INITIAL_POOL_SIZE);
    private static final MpmcArrayQueue<ByteBuffer> FREED_BYTE_BUFFERS = new MpmcArrayQueue<>(INITIAL_POOL_SIZE);

    @Override
    public ThreadLocalBuffer allocate(int minSize) {
        ThreadLocalBufferAllocator allocator = THREAD_LOCAL_IO_BUFFER_ALLOCATORS.get();
        if (allocator == null) {
            allocator = BufferAllocatorFactory.createNotGrowingThreadLocal(INITIAL_POOL_SIZE, this);
            THREAD_LOCAL_IO_BUFFER_ALLOCATORS.set(allocator);
        }

        ThreadLocalBuffer oldIOBuffer;
        while (allocator.hasSpaceForIOBuffer() && (oldIOBuffer = FREED_IO_BUFFERS.poll()) != null) {
            allocator.freeExternalWithoutByteBuffers(oldIOBuffer);
        }

        ByteBuffer oldByteBuffer;
        while (allocator.hasSpaceForByteBuffer() && (oldByteBuffer = FREED_BYTE_BUFFERS.poll()) != null) {
            allocator.free(oldByteBuffer);
        }

        return allocator.allocate(minSize);
    }

    @Override
    public void free(ThreadLocalBuffer ioBuffer) {
        ThreadLocalBufferAllocator allocator = THREAD_LOCAL_IO_BUFFER_ALLOCATORS.get();

        int chunkToRelease = ioBuffer.chunkToRelease();
        boolean ioBufferFreed = false;

        if (allocator != null) {
            if (allocator.hasSpaceForIOBuffer()) {
                allocator.freeExternalWithoutByteBuffers(ioBuffer);
                ioBufferFreed = true;
            }

            for (; chunkToRelease < ioBuffer.chunks.length; chunkToRelease++) {
                if (!allocator.hasSpaceForByteBuffer()) {
                    break;
                }
                allocator.free(ioBuffer.chunks[chunkToRelease]);
            }
        }

        if (!ioBufferFreed) {
            FREED_IO_BUFFERS.offer(ioBuffer);
        }

        for (; chunkToRelease < ioBuffer.chunks.length; chunkToRelease++) {
            ByteBuffer chunk = ioBuffer.chunks[chunkToRelease];
            if (!FREED_BYTE_BUFFERS.offer(chunk)) {
                return;
            }
        }
    }

    @Override
    public void free(ByteBuffer chunk) {
        ThreadLocalBufferAllocator allocator = THREAD_LOCAL_IO_BUFFER_ALLOCATORS.get();
        if (allocator != null && allocator.hasSpaceForByteBuffer()) {
            allocator.free(chunk);
            return;
        }
        FREED_BYTE_BUFFERS.offer(chunk);
    }
}

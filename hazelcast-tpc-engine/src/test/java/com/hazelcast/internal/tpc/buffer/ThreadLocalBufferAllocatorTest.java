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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ThreadLocalBufferAllocatorTest {

    private static final int DEFAULT_BUFFER_SIZE = 1 << 8;

    private ThreadLocalBufferAllocator allocator;

    @Test
    public void when_freeingGrowingAllocator_then_bufferWentToPool() {
        allocator = BufferAllocatorFactory.createGrowingThreadLocal();

        assertEquals(0, allocator.ioBufferPoolPos);

        // Allocate one buffer
        ThreadLocalBuffer buffer = allocator.allocate();
        assertEquals(0, allocator.ioBufferPoolPos);

        // Reclaim buffer to pool.
        allocator.free(buffer);
        assertEquals(1, allocator.ioBufferPoolPos);

        ThreadLocalBuffer pooledBuffer = allocator.bufferPool[0];
        assertEquals(buffer, pooledBuffer);
    }

    @Test
    public void when_freeingNonGrowingAllocator_then_bufferWentToPool() {
        allocator = BufferAllocatorFactory.createNotGrowingThreadLocal(DEFAULT_BUFFER_SIZE, null);

        assertEquals(0, allocator.ioBufferPoolPos);

        // Allocate one buffer
        ThreadLocalBuffer buffer = allocator.allocate();
        assertEquals(0, allocator.ioBufferPoolPos);

        // Reclaim buffer to pool.
        allocator.free(buffer);
        assertEquals(1, allocator.ioBufferPoolPos);

        ThreadLocalBuffer pooledBuffer = allocator.bufferPool[0];
        assertEquals(buffer, pooledBuffer);
    }

    @Test
    public void when_overAllocatingNonGrowingAllocator_bufferPoolDoesntGrow() {
        allocator = BufferAllocatorFactory.createNotGrowingThreadLocal(1, null);

        ThreadLocalBuffer first = allocator.allocate();
        ThreadLocalBuffer second = allocator.allocate();
        assertNotEquals(first, second);
        assertEquals(1, allocator.bufferPool.length);
    }

    @Test
    public void when_allocAndFreeByNonGrowingAllocator_buffersAreSame() {
        allocator = BufferAllocatorFactory.createNotGrowingThreadLocal(1, null);

        ThreadLocalBuffer buf = allocator.allocate();
        ThreadLocalBuffer other = allocator.allocate();

        allocator.free(buf);
        assertEquals(buf, allocator.allocate());
        allocator.free(other);
        assertEquals(other, allocator.allocate());

        allocator.free(other);
        allocator.free(buf);
        assertNotEquals(buf, allocator.allocate());
    }
}

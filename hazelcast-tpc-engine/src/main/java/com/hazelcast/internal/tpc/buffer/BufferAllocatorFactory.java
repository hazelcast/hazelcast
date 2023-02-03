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

import static com.hazelcast.internal.tpc.buffer.ThreadLocalBufferAllocator.INITIAL_POOL_SIZE;

public abstract class BufferAllocatorFactory {
    private static final ConcurrentBufferAllocator CONCURRENT_IO_BUFFER_ALLOCATOR = new ConcurrentBufferAllocator();

    private BufferAllocatorFactory() {
    }

    public static ThreadLocalBufferAllocator createGrowingThreadLocal() {
        return new ThreadLocalBufferAllocator(true, INITIAL_POOL_SIZE, null);
    }

    public static ThreadLocalBufferAllocator createNotGrowingThreadLocal(
            int maxPoolSize,
            ConcurrentBufferAllocator concurrentAllocator
    ) {
        return new ThreadLocalBufferAllocator(false, maxPoolSize, concurrentAllocator);
    }

    public static ConcurrentBufferAllocator createConcurrentAllocator() {
        return CONCURRENT_IO_BUFFER_ALLOCATOR;
    }
}

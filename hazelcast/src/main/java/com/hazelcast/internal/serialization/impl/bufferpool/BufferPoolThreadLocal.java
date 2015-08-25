/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.bufferpool;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.util.ConcurrentReferenceHashMap;

import java.util.concurrent.ConcurrentMap;

/**
 * A thread-local like structure for {@link BufferPoolImpl}.
 *
 * The reason not a regular ThreadLocal is being used is that the pooled instances should be
 * pooled per HZ instance because:
 * - buffers can contain references to hz instance specifics
 * - buffers for a given hz instance should be cleanable when the instance shuts down and we don't
 * want to have memory leaks.
 */
public final class BufferPoolThreadLocal {

    private static final float LOAD_FACTOR = 0.91f;

    private final ConcurrentMap<Thread, BufferPool> pools;
    private final InternalSerializationService serializationService;
    private final BufferPoolFactory bufferPoolFactory;

    public BufferPoolThreadLocal(InternalSerializationService serializationService, BufferPoolFactory bufferPoolFactory) {
        this.serializationService = serializationService;
        this.bufferPoolFactory = bufferPoolFactory;
        int initialCapacity = Runtime.getRuntime().availableProcessors();
        this.pools = new ConcurrentReferenceHashMap<Thread, BufferPool>(initialCapacity, LOAD_FACTOR, 1);
    }

    public BufferPool get() {
        Thread t = Thread.currentThread();
        BufferPool pool = pools.get(t);
        if (pool == null) {
            pool = bufferPoolFactory.create(serializationService);
            pools.put(t, pool);
        }

        return pool;
    }

    public void clear() {
        pools.clear();
    }
}

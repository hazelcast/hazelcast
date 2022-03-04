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

package com.hazelcast.internal.serialization.impl.bufferpool;

import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.ConcurrentReferenceHashMap;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ConcurrentReferenceHashMap.ReferenceType.STRONG;
import static com.hazelcast.internal.util.ConcurrentReferenceHashMap.ReferenceType.WEAK;

/**
 * A thread-local for {@link BufferPool}.
 *
 * The BufferPoolThreadLocal is not assigned to a static field, but is a member field of the SerializationService instance.
 * It is not like the common use of a ThreadLocal where it's assigned to a static field. The reason for this is that the
 * BufferPool instance belongs to a specific SerializationService instance (the buffer pool has buffers tied to a particular
 * SerializationService instance). By creating a ThreadLocal per SerializationService-instance, we obtain 'BufferPool per thread
 * per SerializationService-instance' semantics.
 *
 * <h1>BufferPool is tied to SerializationService-instance</h1>
 * The BufferPool contains e.g. {@link BufferObjectDataOutput} instances that have a reference to a specific
 * SerializationService instance. So as long as there is a strong reference to the BufferPool, there is a strong reference
 * to the SerializationService instance.
 *
 * <h1>The problem with regular ThreadLocals</h1>
 * In the Thread.threadLocal-map only the key (the ThreadLocal) is wrapped in a WeakReference. So when the ThreadLocal instance
 * dies, the WeakReference prevents a strong reference to the ThreadLocal and potentially at some point in the future, the map
 * entry with the null-valued WeakReference-key and the strong reference to the value is removed. The problem is we have
 * no control when this happens and we could end up with a memory leak because there are strong references to e.g. the
 * SerializationService.
 *
 * <h1>BufferPoolThreadLocal wraps BufferPool also in WeakReference</h1>
 * To prevent this memory leak, the value (the BufferPool) in the Thread.threadLocals-map is also wrapped in a WeakReference
 * So if there is no strong reference to the BufferPool, the BufferPool is gc'ed. Even though there might be some empty key/value
 * WeakReferences in the Thread.threadLocal map.
 *
 * <h1>Strong references to the BufferPools</h1>
 * To prevent the BufferPool wrapped in a WeakReference to be gc'ed as soon as it is created, the BufferPool is also stored
 * in the BufferPoolThreadLocal in a ConcurrentReferenceHashMap with a weak reference for the key (the thread) and a
 * strong reference to the value (BufferPool):
 *
 * This gives us the following behavior:
 * <ol>
 * <li>
 * As soon as the Thread, having the ThreadLocal in its Thread.threadLocals map, dies, the Entry with the Thread as key
 * will be removed from the ConcurrentReferenceHashMap at some point.
 * </li>
 * <li>
 * As soon as the SerializationService is collected, the BufferPoolThreadLocal is collected. And because of this, there
 * are no strong references to the BufferPool instances, and they get collected as well.
 * </li>
 * </ol>
 * So it can be that a Thread in its Thread.threadLocal map will for some time have an empty weak reference as key and value. But
 * there won't be any references to the BufferPool/SerializationService.
 *
 * <h1>Performance</h1>
 * The Performance of using a ThreadLocal in combination with a WeakReference is almost the same as using a ThreadLocal without
 * WeakReference. There is an extra pointer indirection and some additional pressure on the gc system since it needs to deal with
 * the WeakReferences, but the number of threads is limited.
 */
public final class BufferPoolThreadLocal {

    private final ThreadLocal<WeakReference<BufferPool>> threadLocal = new ThreadLocal<>();
    private final InternalSerializationService serializationService;
    private final BufferPoolFactory bufferPoolFactory;
    private final Map<Thread, BufferPool> strongReferences = new ConcurrentReferenceHashMap<>(WEAK, STRONG);
    private final Supplier<RuntimeException> notActiveExceptionSupplier;

    public BufferPoolThreadLocal(InternalSerializationService serializationService,
                                 BufferPoolFactory bufferPoolFactory,
                                 Supplier<RuntimeException> notActiveExceptionSupplier) {
        this.serializationService = serializationService;
        this.bufferPoolFactory = bufferPoolFactory;
        this.notActiveExceptionSupplier = notActiveExceptionSupplier;
    }

    public BufferPool get() {
        WeakReference<BufferPool> ref = threadLocal.get();
        if (ref == null) {
            BufferPool pool = bufferPoolFactory.create(serializationService);
            ref = new WeakReference<>(pool);
            strongReferences.put(Thread.currentThread(), pool);
            threadLocal.set(ref);
            return pool;
        } else {
            BufferPool pool = ref.get();
            if (pool == null) {
                throw notActiveExceptionSupplier.get();
            }
            return pool;
        }
    }

    public void clear() {
        strongReferences.clear();
    }
}

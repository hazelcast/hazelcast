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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.internal.serialization.SerializableByConvention;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SerializableByConvention
public final class InvalidationQueue<T> extends ConcurrentLinkedQueue<T> {
    private final AtomicInteger elementCount = new AtomicInteger(0);
    private final AtomicBoolean flushingInProgress = new AtomicBoolean(false);

    @Override
    public int size() {
        return elementCount.get();
    }

    @Override
    public boolean offer(T invalidation) {
        boolean offered = super.offer(invalidation);
        if (offered) {
            elementCount.incrementAndGet();
        }
        return offered;
    }

    @Override
    public T poll() {
        T invalidation = super.poll();
        if (invalidation != null) {
            elementCount.decrementAndGet();
        }
        return invalidation;
    }

    public boolean tryAcquire() {
        return flushingInProgress.compareAndSet(false, true);
    }

    public void release() {
        flushingInProgress.set(false);
    }

    @Override
    public boolean add(T invalidation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }
}

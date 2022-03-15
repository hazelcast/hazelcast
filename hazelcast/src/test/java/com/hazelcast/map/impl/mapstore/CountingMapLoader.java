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

package com.hazelcast.map.impl.mapstore;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class CountingMapLoader extends SimpleMapLoader {

    private AtomicInteger loadedValueCount = new AtomicInteger();
    private AtomicInteger loadAllKeysInvocations = new AtomicInteger();
    private AtomicBoolean loadAllKeysClosed = new AtomicBoolean();

    CountingMapLoader(int size) {
        super(size, false);
    }

    CountingMapLoader(int size, boolean slow) {
        super(size, slow);
    }

    @Override
    public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
        loadedValueCount.addAndGet(keys.size());
        return super.loadAll(keys);
    }

    public int getLoadedValueCount() {
        return loadedValueCount.get();
    }

    public void reset() {
        loadedValueCount.set(0);
        loadAllKeysClosed.set(false);
    }

    public boolean isLoadAllKeysClosed() {
        return loadAllKeysClosed.get();
    }

    @Override
    public Iterable<Integer> loadAllKeys() {
        final Iterable<Integer> allKeys = super.loadAllKeys();
        loadAllKeysInvocations.incrementAndGet();
        return () -> new CloseableIterator<>(allKeys.iterator());
    }

    public int getLoadAllKeysInvocations() {
        return loadAllKeysInvocations.get();
    }

    private class CloseableIterator<T> implements Iterator<T>, Closeable {

        private Iterator<T> iterator;

        CloseableIterator(Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            return iterator.next();
        }

        @Override
        public void remove() {
        }

        @Override
        public void close() {
            loadAllKeysClosed.set(true);
        }
    }
}

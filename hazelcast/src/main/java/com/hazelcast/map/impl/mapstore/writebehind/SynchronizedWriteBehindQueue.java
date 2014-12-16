/*
* Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import java.util.Collection;
import java.util.List;

/**
 * Wrapper for a not thread safe {@link WriteBehindQueue},
 * only provides thread-safe access, if all accesses to the underlying wrapped {@link WriteBehindQueue}
 * are from an instance of this wrapper sync queue.
 *
 * @param <E> Type of entry to be stored.
 */
class SynchronizedWriteBehindQueue<E> implements WriteBehindQueue<E> {

    private final WriteBehindQueue<E> queue;

    private final Object mutex;

    SynchronizedWriteBehindQueue(WriteBehindQueue<E> queue) {
        if (queue == null) {
            throw new NullPointerException();
        }
        this.queue = queue;
        this.mutex = this;
    }

    @Override
    public boolean offer(E e) {
        synchronized (mutex) {
            return queue.offer(e);
        }
    }

    @Override
    public E get(E e) {
        synchronized (mutex) {
            return queue.get(e);
        }
    }

    @Override
    public E getFirst() {
        synchronized (mutex) {
            return queue.getFirst();
        }
    }

    @Override
    public void removeFirst() {
        synchronized (mutex) {
            queue.removeFirst();
        }
    }


    @Override
    public int size() {
        synchronized (mutex) {
            return queue.size();
        }
    }

    @Override
    public void clear() {
        synchronized (mutex) {
            queue.clear();
        }
    }

    @Override
    public WriteBehindQueue<E> getSnapShot() {
        synchronized (mutex) {
            return new SynchronizedWriteBehindQueue<E>(queue.getSnapShot());
        }
    }

    @Override
    public void addFront(Collection<E> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        synchronized (mutex) {
            queue.addFront(collection);
        }
    }

    @Override
    public void addEnd(Collection<E> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        synchronized (mutex) {
            queue.addEnd(collection);
        }
    }

    @Override
    public void removeAll(Collection<E> collection) {
        synchronized (mutex) {
            queue.removeAll(collection);
        }
    }

    @Override
    public List<E> removeAll() {
        synchronized (mutex) {
            return queue.removeAll();
        }
    }

    @Override
    public boolean isEnabled() {
        synchronized (mutex) {
            return queue.isEnabled();
        }
    }

    @Override
    public List<E> asList() {
        synchronized (mutex) {
            return queue.asList();
        }
    }

    @Override
    public List<E> filterItems(long now) {
        synchronized (mutex) {
            return queue.filterItems(now);
        }
    }

}

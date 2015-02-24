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
    public void addFirst(Collection<E> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        synchronized (mutex) {
            queue.addFirst(collection);
        }
    }

    @Override
    public void addLast(E e) {
        synchronized (mutex) {
            queue.addLast(e);
        }
    }

    /**
     * Removes the first occurrence of the specified element in this queue
     * when searching it by starting from the head of this queue.
     *
     * @param e element to be removed.
     * @return <code>true</code> if removed successfully, <code>false</code> otherwise
     */
    @Override
    public boolean removeFirstOccurrence(E e) {
        synchronized (mutex) {
            return queue.removeFirstOccurrence(e);
        }
    }

    @Override
    public boolean contains(E e) {
        synchronized (mutex) {
            return queue.contains(e);
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
    public int drainTo(Collection<E> collection) {
        synchronized (mutex) {
            return queue.drainTo(collection);
        }
    }

    @Override
    public List<E> asList() {
        synchronized (mutex) {
            return queue.asList();
        }
    }

    @Override
    public void getFrontByTime(long time, Collection<E> collection) {
        synchronized (mutex) {
            queue.getFrontByTime(time, collection);
        }
    }

    @Override
    public void getFrontByNumber(int numberOfElements, Collection<E> collection) {
        synchronized (mutex) {
            queue.getFrontByNumber(numberOfElements, collection);
        }
    }

}

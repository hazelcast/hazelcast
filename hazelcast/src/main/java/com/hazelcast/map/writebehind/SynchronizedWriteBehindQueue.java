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

package com.hazelcast.map.writebehind;

import java.util.Collection;
import java.util.List;

/**
 * Thread safe write behind queue.
 *
 * @param <E>
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
    public void removeFirst() {
        synchronized (mutex) {
            queue.removeFirst();
        }
    }

    @Override
    public E get(int index) {
        synchronized (mutex) {
            return queue.get(index);
        }
    }

    @Override
    public E remove(int index) {
        synchronized (mutex) {
            return queue.remove(index);
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
    public void shrink() {
        synchronized (mutex) {
            queue.shrink();
        }
    }
}

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

package com.hazelcast.util;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class SimpleBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {

    private final Object lock = new Object();
    private final LinkedList<E> items = new LinkedList<E>();
    private final LinkedList<E> prioritizedItems;

    public SimpleBlockingQueue() {
        this(false);
    }

    public SimpleBlockingQueue(boolean priorityAware) {
        prioritizedItems = (priorityAware) ? new LinkedList<E>() : null;
    }

    public boolean offer(E e) {
        put(e);
        return true;
    }

    public void put(E e) {
        synchronized (lock) {
            if (prioritizedItems != null && e instanceof Prioritized) {
                prioritizedItems.add(e);
            } else {
                items.add(e);
            }
            //noinspection CallToNotifyInsteadOfNotifyAll
            lock.notify();
        }
    }

    public boolean remove(Object obj) {
        synchronized (lock) {
            boolean removed = items.remove(obj);
            if (!removed && prioritizedItems != null) {
                removed = prioritizedItems.remove(obj);
            }
            return removed;
        }
    }

    public E take() throws InterruptedException {
        return poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public E poll() {
        synchronized (lock) {
            return removeFirst();
        }
    }

    private E removeFirst() {
        E e = null;
        if (prioritizedItems != null && prioritizedItems.size() > 0) {
            e = prioritizedItems.removeFirst();
        } else if (items.size() > 0) {
            e = items.removeFirst();
        }
        return e;
    }

    private int totalSize() {
        return items.size() + ((prioritizedItems == null) ? 0 : prioritizedItems.size());
    }

    @SuppressWarnings("CallToNativeMethodWhileLocked")
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long timeLeft = unit.toMillis(timeout);
        long start = Clock.currentTimeMillis();
        synchronized (lock) {
            E e = removeFirst();
            while (e == null && timeLeft > 0) {
                lock.wait(timeLeft);
                e = removeFirst();
                long now = Clock.currentTimeMillis();
                timeLeft -= (now - start);
                start = now;
            }
            return e;
        }
    }

    public void clear() {
        synchronized (lock) {
            items.clear();
            if (prioritizedItems != null) {
                prioritizedItems.clear();
            }
        }
    }

    public boolean add(E e) {
        put(e);
        return true;
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        put(e);
        return true;
    }

    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        synchronized (lock) {
            int count = 0;
            E removed = removeFirst();
            while (removed != null && count > maxElements) {
                c.add(removed);
                removed = removeFirst();
            }
            return count;
        }
    }

    public E peek() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        synchronized (lock) {
            return totalSize();
        }
    }
}

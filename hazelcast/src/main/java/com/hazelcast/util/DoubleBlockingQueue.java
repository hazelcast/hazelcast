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
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class DoubleBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private final Object notEmptyLock = new Object();
    private final Queue<E> priorityQueue = new ConcurrentLinkedQueue<E>();
    private final Queue<E> defaultQueue = new ConcurrentLinkedQueue<E>();

    public void put(E e) throws InterruptedException {
        defaultQueue.offer(e);
        synchronized (notEmptyLock) {
            //noinspection CallToNotifyInsteadOfNotifyAll
            notEmptyLock.notify();
        }
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        put(e);
        return true;
    }

    @Override
    public int size() {
        return priorityQueue.size() + defaultQueue.size();
    }

    public E take() throws InterruptedException {
        return poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E e = tryPoll();
        if (e != null) return e;
        long timeLeft = unit.toMillis(timeout);
        while (e == null && timeLeft > 0) {
            long start = Clock.currentTimeMillis();
            synchronized (notEmptyLock) {
                notEmptyLock.wait(100);
            }
            e = tryPoll();
            long now = Clock.currentTimeMillis();
            timeLeft -= (now - start);
            start = now;
        }
        return e;
    }

    E tryPoll() {
        E e = priorityQueue.poll();
        if (e == null) {
            e = defaultQueue.poll();
        }
        return e;
    }

    public E poll() {
        return tryPoll();
    }

    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    public int drainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException();
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    public boolean offer(E e) {
        return defaultQueue.offer(e);
    }

    public E peek() {
        throw new UnsupportedOperationException();
    }
}

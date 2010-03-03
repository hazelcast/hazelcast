/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.util;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class UnboundedBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {

    private final Object lock = new Object();
    private final ConcurrentLinkedQueue<E> queue = new ConcurrentLinkedQueue<E>();

    public boolean offer(E e) {
        put(e);
        return true;
    }

    public boolean remove(Object obj) {
        return queue.remove(obj);
    }

    public void put(E e) {
        queue.add(e);
        synchronized (lock) {
            lock.notify();
        }
    }

    public E take() throws InterruptedException {
        return poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public E poll() {
        return queue.poll();
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E e = queue.poll();
        long timeLeft = unit.toMillis(timeout);
        synchronized (lock) {
            while (e == null && timeLeft > 0) {
                long now = System.currentTimeMillis();
                lock.wait(Math.min(100, timeLeft));
                timeLeft -= (System.currentTimeMillis() - now);
                e = queue.poll();
            }
        }
        return e;
    }

    public void clear() {
        queue.clear();
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(e);
    }

    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    public int drainTo(Collection<? super E> c) {
        E e = queue.poll();
        int count = 0;
        while (e != null) {
            c.add(e);
            count++;
            e = queue.poll();
        }
        return count;
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        E e = queue.poll();
        int count = 0;
        while (e != null && count < maxElements) {
            c.add(e);
            count++;
            e = queue.poll();
        }
        return count;
    }

    public E peek() {
        return queue.peek();
    }

    @Override
    public Iterator<E> iterator() {
        return queue.iterator();
    }

    @Override
    public int size() {
        return queue.size();
    }
}
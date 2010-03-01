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

public class UnboundedBlockingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {

    private final Object lock = new Object();
    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<T>();

    public boolean offer(T t) {
        put(t);
        return true;
    }

    public boolean remove(Object obj) {
        return queue.remove(obj);
    }

    public void put(T t) {
        queue.add(t);
        synchronized (lock) {
            lock.notify();
        }
    }

    public T take() throws InterruptedException {
        return poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public T poll() {
        return queue.poll();
    }

    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        T t = queue.poll();
        long timeLeft = unit.toMillis(timeout);
        synchronized (lock) {
            while (t == null && timeLeft > 0) {
                long now = System.currentTimeMillis();
                lock.wait(Math.min(100, timeLeft));
                timeLeft -= (System.currentTimeMillis() - now);
                t = queue.poll();
            }
        }
        return t;
    }

    public void clear() {
        queue.clear();
    }

    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(t);
    }

    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    public int drainTo(Collection<? super T> c) {
        T t = queue.poll();
        int count = 0;
        while (t != null) {
            c.add(t);
            count++;
            t = queue.poll();
        }
        return count;
    }

    public int drainTo(Collection<? super T> c, int maxElements) {
        T t = queue.poll();
        int count = 0;
        while (t != null && count < maxElements) {
            c.add(t);
            count++;
            t = queue.poll();
        }
        return count;
    }

    public T peek() {
        return queue.peek();
    }

    @Override
    public Iterator<T> iterator() {
        return queue.iterator();
    }

    @Override
    public int size() {
        return queue.size();
    }
}
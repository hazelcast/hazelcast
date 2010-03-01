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
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class SimpleResponseQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {

    private final Object lock = new Object();
    private final LinkedList<T> results = new LinkedList<T>();

    public boolean offer(T t) {
        put(t);
        return true;
    }

    public void put(T t) {
        synchronized (lock) {
            results.add(t);
            lock.notify();
        }
    }

    public T take() throws InterruptedException {
        return poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public T poll() {
        synchronized (lock) {
            if (results.size() == 0) {
                return null;
            } else {
                return results.removeFirst();
            }
        }
    }

    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        synchronized (lock) {
            long timeLeft = unit.toMillis(timeout);
            while (results.size() == 0 && timeLeft > 0) {
                long start = System.currentTimeMillis();
                lock.wait(timeLeft);
                if (results.size() > 0) {
                    return results.removeFirst();
                }
                timeLeft -= (System.currentTimeMillis() - start);
            }
            if (results.size() == 0) {
                return null;
            } else {
                return results.removeFirst();
            }
        }
    }

    public void clear() {
        synchronized (lock) {
            results.clear();
        }
    }

    public boolean add(T t) {
        put(t);
        return true;
    }

    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        put(t);
        return true;
    }

    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    public int drainTo(Collection<? super T> c) {
        throw new UnsupportedOperationException();
    }

    public int drainTo(Collection<? super T> c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    public T peek() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        synchronized (lock) {
            return results.size();
        }
    }
}
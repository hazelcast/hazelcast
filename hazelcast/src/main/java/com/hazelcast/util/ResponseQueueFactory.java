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
import java.util.concurrent.TimeUnit;

public class ResponseQueueFactory {
    public static BlockingQueue newResponseQueue() {
        return new ResponseQueue();
    }

    private final static class ResponseQueue extends AbstractQueue implements BlockingQueue {
        volatile Object response;
        final Object lock = new Object();

        public Object take() throws InterruptedException {
            while (response == null) {
                synchronized (lock) {
                    if (response == null) {
                        lock.wait();
                    }
                }
            }
            return response;
        }

        public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException {
            return offer(o);
        }

        public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
            if (timeout == 0) return response;
            if (timeout < 0) new IllegalArgumentException();
            long remaining = unit.toMillis(timeout);
            while (response == null && remaining > 0) {
                synchronized (lock) {
                    if (response == null) {
                        long start = System.currentTimeMillis();
                        lock.wait(remaining);
                        remaining -= (System.currentTimeMillis() - start);
                    }
                }
            }
            return response;
        }

        public void put(Object o) throws InterruptedException {
            offer(o);
        }

        public boolean offer(Object obj) {
            if (this.response != null) {
                return false;
            }
            this.response = obj;
            synchronized (lock) {
                lock.notify();
            }
            return true;
        }

        public Object poll() {
            return response;
        }

        public int remainingCapacity() {
            throw new UnsupportedOperationException();
        }

        public int drainTo(Collection c) {
            throw new UnsupportedOperationException();
        }

        public int drainTo(Collection c, int maxElements) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            response = null;
        }

        @Override
        public Iterator iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return (response == null) ? 0 : 1;
        }

        public Object peek() {
            return response;
        }
    }
}

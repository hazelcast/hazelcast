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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Factory for creating response queues.
 * <p/>
 * See LockBasedResponseQueue.
 */
public final class ResponseQueueFactory {

    private ResponseQueueFactory() {
    }

    /**
     * Creates new response queue
     * @return LockBasedResponseQueue new created response queue
     */
    public static BlockingQueue newResponseQueue() {
        return new LockBasedResponseQueue();
    }

    private static final class LockBasedResponseQueue extends AbstractQueue implements BlockingQueue {
        private static final Object NULL = new Object();
        private Object response;
        private final Lock lock = new ReentrantLock();
        private final Condition noValue = lock.newCondition();


        public Object take() throws InterruptedException {
            lock.lock();
            try {
                //noinspection WhileLoopSpinsOnField
                while (response == null) {
                    noValue.await();
                }
                return getAndRemoveResponse();
            } finally {
                lock.unlock();
            }
        }

        public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException {
            return offer(o);
        }

        public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
            if (timeout < 0) {
                throw new IllegalArgumentException();
            }
            long remaining = unit.toMillis(timeout);
            lock.lock();
            try {
                boolean timedOut = false;
                while (response == null && remaining > 0 && !timedOut) {
                    long start = Clock.currentTimeMillis();
                    timedOut = noValue.await(remaining, TimeUnit.MILLISECONDS);
                    remaining -= (Clock.currentTimeMillis() - start);
                }
                return getAndRemoveResponse();
            } finally {
                lock.unlock();
            }
        }

        public void put(Object o) throws InterruptedException {
            offer(o);
        }

        public boolean offer(Object obj) {
            Object item = obj;
            if (item == null) {
                item = NULL;
            }
            lock.lock();
            try {
                if (response != null) {
                    return false;
                }
                response = item;
                //noinspection CallToSignalInsteadOfSignalAll
                noValue.signal();
                return true;
            } finally {
                lock.unlock();
            }
        }

        public Object poll() {
            lock.lock();
            try {
                return getAndRemoveResponse();
            } finally {
                lock.unlock();
            }
        }

        /**
         * Internal method, should be called under lock.
         *
         * @return response
         */
        private Object getAndRemoveResponse() {
            final Object value = response;
            response = null;
            return (value == NULL) ? null : value;
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
            lock.lock();
            try {
                response = null;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Iterator iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            lock.lock();
            try {
                return (response == null) ? 0 : 1;
            } finally {
                lock.unlock();
            }
        }

        public Object peek() {
            lock.lock();
            try {
                return response;
            } finally {
                lock.unlock();
            }
        }
    }
}

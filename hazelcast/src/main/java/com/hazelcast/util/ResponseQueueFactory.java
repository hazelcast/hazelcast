/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 *
 * See LockBasedResponseQueue.
 */
public final class ResponseQueueFactory {

    private ResponseQueueFactory() {
    }

    /**
     * Creates new response queue
     *
     * @return LockBasedResponseQueue new created response queue
     */
    public static BlockingQueue<Object> newResponseQueue() {
        return new LockBasedResponseQueue();
    }

    private static final class LockBasedResponseQueue extends AbstractQueue<Object> implements BlockingQueue<Object> {

        private static final Object NULL = new Object();

        private final Lock lock = new ReentrantLock();
        private final Condition noValue = lock.newCondition();

        private Object response;

        @Override
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

        @Override
        public boolean offer(Object object, long timeout, TimeUnit unit) throws InterruptedException {
            return offer(object);
        }

        @Override
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

        @Override
        public void put(Object object) throws InterruptedException {
            offer(object);
        }

        @Override
        public boolean offer(Object object) {
            Object item = object;
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

        @Override
        public Object poll() {
            lock.lock();
            try {
                return getAndRemoveResponse();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int remainingCapacity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int drainTo(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int drainTo(Collection c, int maxElements) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            lock.lock();
            try {
                response = null;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Iterator<Object> iterator() {
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

        @Override
        public Object peek() {
            lock.lock();
            try {
                return response;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Internal method, should be called under lock.
         */
        private Object getAndRemoveResponse() {
            Object value = response;
            response = null;
            return (value == NULL ? null : value);
        }
    }
}

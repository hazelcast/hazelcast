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

import com.hazelcast.core.HazelcastException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @mdogan 12/3/12
 */
public class SpinReadWriteLock {

    private final long spinInterval; // in ms

    private final AtomicBoolean locked = new AtomicBoolean(false);

    private final AtomicInteger readCount = new AtomicInteger();

    public SpinReadWriteLock() {
        this.spinInterval = 1;
    }

    public SpinReadWriteLock(int spinInterval, TimeUnit unit) {
        final long millis = unit.toMillis(spinInterval);
        this.spinInterval = millis > 0 ? millis : 1;
    }

    public SpinLock readLock() {
        return new ReadLock();
    }

    public SpinLock writeLock() {
        return new WriteLock();
    }

    private boolean acquireReadLock(final long time, TimeUnit unit) throws InterruptedException {
        final long timeInMillis = unit.toMillis(time);
        long elapsed = 0L;
        while (locked.get()) {
            Thread.sleep(spinInterval);
            if ((elapsed += spinInterval) > timeInMillis) {
                return false;
            }
        }
        readCount.incrementAndGet();
        if (locked.get()) {
            readCount.decrementAndGet();
            return acquireReadLock(timeInMillis - elapsed, TimeUnit.MILLISECONDS);
        }
        return true;
    }

    private void releaseReadLock() {
        readCount.decrementAndGet();
    }

    private void acquireWriteLock() throws InterruptedException {
        while (!locked.compareAndSet(false, true)) {
            Thread.sleep(spinInterval);
        }
        while (readCount.get() > 0) {
            Thread.sleep(spinInterval);
        }
        // go on ...
    }

    private void releaseWriteLock() {
        if (!locked.getAndSet(false)) {
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        }
    }

    private class ReadLock implements SpinLock {

        public void lock() {
            try {
                if (!tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                    throw new HazelcastException();
                }
            } catch (InterruptedException e) {
                throw new HazelcastException();
            }
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return acquireReadLock(time, unit);
        }

        public void unlock() {
            releaseReadLock();
        }
    }

    private class WriteLock implements SpinLock {

        public void lock() {
            try {
                acquireWriteLock();
            } catch (InterruptedException e) {
                throw new HazelcastException();
            }
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            acquireWriteLock();
            return true;
        }

        public void unlock() {
            releaseWriteLock();
        }
    }

}

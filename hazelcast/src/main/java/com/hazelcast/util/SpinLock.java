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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @mdogan 3/29/13
 */
public final class SpinLock implements Lock {

    private final long spinInterval; // in ms

    private final AtomicBoolean locked = new AtomicBoolean(false);

    private long tid = -1L;

    private int count = 0;

    public SpinLock() {
        spinInterval = 1;
    }

    public SpinLock(int spinInterval, TimeUnit unit) {
        final long millis = unit.toMillis(spinInterval);
        this.spinInterval = millis > 0 ? millis : 1;
    }

    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            throw new HazelcastException(e);
        }
    }

    public void lockInterruptibly() throws InterruptedException {
        if (!tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
            throw new HazelcastException();
        }
    }

    public boolean tryLock() {
        try {
            return tryLock(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        final Thread currentThread = Thread.currentThread();
        if (currentThread.getId() == tid) {
            count++;
            return true;
        }
        final long timeInMillis = unit.toMillis(time > 0 ? time : 0);
        final long spin = spinInterval;
        long elapsed = 0L;
        while (!locked.compareAndSet(false, true)) {
            Thread.sleep(spin);
            if ((elapsed += spin) > timeInMillis) {
                return false;
            }
        }
        tid = currentThread.getId();
        count = 1;
        return true;
    }

    public void unlock() {
        final Thread currentThread = Thread.currentThread();
        if (currentThread.getId() != tid) {
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        }
        if (count > 1) {
            count--;
            return;
        }
        if (!locked.getAndSet(false)) {
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        }
        tid = -1L;
        count = 0;
    }

    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}

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

package com.hazelcast.query.impl;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base class for concrete index store implementations
 */
public abstract class BaseIndexStore implements IndexStore {

    protected volatile boolean locked = false;
    protected volatile long ownerId = -1;
    protected AtomicLong readerCount = new AtomicLong();
    protected Lock lock = new ReentrantLock();
    protected Condition lockCondition = lock.newCondition();

    protected void takeLock() {
        locked = true;
        ownerId = Thread.currentThread().getId();
        while (readerCount.get() > 0) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        lock.lock();
    }

    protected void releaseLock() {
        ownerId = -1;
        locked = false;
        try {
            lockCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    protected void ensureNotLocked() {
        while (locked && ownerId != Thread.currentThread().getId()) {
            try {
                lockCondition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

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

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base class for concrete index store implementations
 */
public abstract class BaseIndexStore implements IndexStore {

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    protected ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    protected void takeWriteLock() {
        writeLock.lock();
    }

    protected void releaseWriteLock() {
        writeLock.unlock();
    }

    protected void takeReadLock() {
        readLock.lock();
    }

    protected void releaseReadLock() {
        readLock.unlock();
    }
}

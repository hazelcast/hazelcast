/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.lock;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.raft.RaftGroupId;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * TODO: Javadoc Pending...
 * TODO: Move this interface to somewhere like com.hazelcast.core
 */
public interface FencedLock extends Lock, DistributedObject {

    void lock();

    void lockInterruptibly() throws InterruptedException;

    long lockAndGetFence();

    boolean tryLock();

    long tryLockAndGetFence();

    boolean tryLock(long time, @Nonnull TimeUnit unit);

    long tryLockAndGetFence(long time, @Nonnull TimeUnit unit);

    void unlock();

    @Nonnull Condition newCondition();

    void forceUnlock();

    long getFence();

    boolean isLocked();

    boolean isLockedByCurrentThread();

    // returns the true lock count if the lock is acquired by the caller endpoint
    // returns 1 if the lock is acquired by another endpoint because reentrant acquires are local
    // returns 0 otherwise
    int getLockCount();

    RaftGroupId getGroupId();

    String getName();
}

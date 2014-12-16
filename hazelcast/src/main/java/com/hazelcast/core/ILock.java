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

package com.hazelcast.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Distributed implementation of {@link Lock}.
 *
 * <h1>Example</h1>
 * <pre><code>
 *     Lock mylock = Hazelcast.getLock(mylockobject); mylock.lock();
 * try {
 *      // do something
 * } finally {
 *      mylock.unlock();
 * }
 * //Lock on Map
 *      IMap<String, Customer> map = Hazelcast.getMap("customers"); map.lock("1");
 * try {
 *      // do something
 * } finally {
 * map.unlock("1"); }
 * </code></pre>
 *
 * @see Lock
 */
public interface ILock extends Lock, DistributedObject {

    /**
     * Returns the lock object, the key for this lock instance.
     *
     * @return lock object.
     * @deprecated use {@link #getName()} instead.
     */
    @Deprecated
    Object getKey();

    /**
     * {@inheritDoc}
     */
    void lock();

    /**
     * {@inheritDoc}
     */
    boolean tryLock();

    /**
     * {@inheritDoc}
     */
    boolean tryLock(long time, TimeUnit unit) throws InterruptedException;

    /**
     * Releases the lock.
     */
    void unlock();

    /**
     * Acquires the lock for the specified lease time.
     * <p>After lease time, lock will be released..
     * <p/>
     * <p>If the lock is not available then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     * <p/>
     *
     * @param leaseTime time to wait before releasing the lock.
     * @param timeUnit unit of time for the lease time.
     *
     * @throws IllegalMonitorStateException if the current thread does not
     * hold this lock
     */
    void lock(long leaseTime, TimeUnit timeUnit);

    /**
     * Releases the lock regardless of the lock owner.
     * It always successfully unlocks, never blocks, and returns immediately.
     */
    void forceUnlock();

    /**
     * This method is not implemented! Use {@link #newCondition(String)} instead.
     *
     * @throws UnsupportedOperationException
     */
    Condition newCondition();

    /**
     * Returns a new {@link ICondition} instance that is bound to this
     * {@code ILock} instance with given name.
     *
     * <p>Before waiting on the condition the lock must be held by the
     * current thread.
     * A call to {@link ICondition#await()} will atomically release the lock
     * before waiting and re-acquire the lock before the wait returns.
     *
     * @param name identifier of the new condition instance
     * @return A new {@link ICondition} instance for this {@code ILock} instance
     * @throws java.lang.NullPointerException if name is null.
     */
    ICondition newCondition(String name);

    /**
     * Returns whether this lock is locked or not.
     *
     * @return {@code true} if this lock is locked, {@code false} otherwise.
     */
    boolean isLocked();

    /**
     * Returns whether this lock is locked by current thread or not.
     *
     * @return {@code true} if this lock is locked by current thread, {@code false} otherwise.
     */
    boolean isLockedByCurrentThread();

    /**
     * Returns re-entrant lock hold count, regardless of lock ownership.
     *
     * @return the lock hold count.
     */
    int getLockCount();

    /**
     * Returns remaining lease time in milliseconds.
     * If the lock is not locked then -1 will be returned.
     *
     * @return remaining lease time in milliseconds.
     */
    long getRemainingLeaseTime();

}

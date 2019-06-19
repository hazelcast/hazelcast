/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.lock;

import com.hazelcast.core.DistributedObject;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Distributed implementation of {@link Lock}.
 * <h1>Example</h1>
 * <pre><code>
 *     Lock mylock = Hazelcast.getLock(mylockobject); mylock.lock();
 * try {
 *      // do something
 * } finally {
 *      mylock.unlock();
 * }
 * //Lock on Map
 *      IMap&lt;String, Customer&gt; map = Hazelcast.getMap("customers"); map.lock("1");
 * try {
 *      // do something
 * } finally {
 * map.unlock("1"); }
 * </code></pre>
 * Behaviour of {@link ILock} under split-brain scenarios should be taken into account when using this
 * data structure.  During a split, each partitioned cluster will either create a brand new and un-acquired
 * {@link ILock} or it will continue to use the primary or back-up version. As the acquirer of the {@link ILock} might
 * reside in a different partitioned network this can lead to situations where the lock is never obtainable.
 * <p>
 * When the split heals, Hazelcast performs a default largest cluster wins resolution. Where the clusters are
 * the same size a winner of the merge will be randomly chosen. In any case, this can lead to situations where
 * (post-merge) multiple acquirers think they hold the same lock, when in fact the {@link ILock} itself records only one
 * owner.  When the false owners come to release the {@link ILock} an {@link IllegalMonitorStateException} is
 * thrown.
 * <p>
 * Acquiring an {@link ILock} with a lease time {@link #lock(long, TimeUnit)} can help to mitigate such scenarios.
 * <p>
 * As a defensive mechanism against such inconsistency, consider using the in-built
 * <a href="http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#split-brain-protection-for-lock">
 * split-brain protection for lock</a>.  Using this functionality it is possible to restrict operations in smaller
 * partitioned clusters. It should be noted that there is still an inconsistency window between the time of
 * the split and the actual detection.  Therefore using this reduces the window of inconsistency but can never
 * completely eliminate it.
 *
 * @see Lock
 * @see com.hazelcast.cp.lock.FencedLock
 * @deprecated Please use {@link com.hazelcast.cp.lock.FencedLock} instead.
 * This interface will be removed in Hazelcast 4.0.
 */
@Deprecated
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
     * Tries to acquire the lock for the specified lease time.
     * <p>
     * After lease time, the lock will be released.
     * <p>
     * If the lock is not available, then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of two things happens:
     * <ul>
     * <li>the lock is acquired by the current thread, or</li>
     * <li>the specified waiting time elapses.</li>
     * </ul>
     *
     * @param time      maximum time to wait for the lock.
     * @param unit      time unit of the <code>time</code> argument.
     * @param leaseTime time to wait before releasing the lock.
     * @param leaseUnit unit of time to specify lease time.
     * @return <code>true</code> if the lock was acquired and <code>false</code>
     * if the waiting time elapsed before the lock was acquired.
     */
    boolean tryLock(long time, TimeUnit unit, long leaseTime, TimeUnit leaseUnit) throws InterruptedException;

    /**
     * Releases the lock.
     */
    void unlock();

    /**
     * Acquires the lock for the specified lease time.
     * <p>
     * After lease time, lock will be released..
     * <p>
     * If the lock is not available then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     *
     * @param leaseTime time to wait before releasing the lock.
     * @param timeUnit  unit of time for the lease time.
     * @throws IllegalMonitorStateException if the current thread does not hold this lock
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
     * @throws UnsupportedOperationException the exception is always thrown, since this method is not implemented
     */
    Condition newCondition();

    /**
     * Returns a new {@link ICondition} instance that is bound to this
     * {@code ILock} instance with given name.
     * <p>
     * Before waiting on the condition the lock must be held by the
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

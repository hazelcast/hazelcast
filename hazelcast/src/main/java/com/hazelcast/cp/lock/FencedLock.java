/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.exception.LockAcquireLimitReachedException;
import com.hazelcast.cp.lock.exception.LockOwnershipLostException;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.cp.session.CPSessionManagementService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A linearizable &amp; distributed &amp; reentrant implementation of {@link Lock}.
 * <p>
 * {@link FencedLock} is accessed via {@link CPSubsystem#getLock(String)}.
 * <p>
 * {@link FencedLock} is CP with respect to the CAP principle. It works on top
 * of the Raft consensus algorithm. It offers linearizability during crash-stop
 * failures and network partitions. If a network partition occurs, it remains
 * available on at most one side of the partition.
 * <p>
 * {@link FencedLock} works on top of CP sessions. Please see {@link CPSession}
 * for more information about CP sessions.
 * <p>
 * By default, {@link FencedLock} is reentrant. Once a caller acquires
 * the lock, it can acquire the lock reentrantly as many times as it wants
 * in a linearizable manner. You can configure the reentrancy behaviour via
 * {@link FencedLockConfig}. For instance, reentrancy can be disabled and
 * {@link FencedLock} can work as a non-reentrant mutex. One can also set
 * a custom reentrancy limit. When the reentrancy limit is reached,
 * {@link FencedLock} does not block a lock call. Instead, it fails with
 * {@link LockAcquireLimitReachedException} or a specified return value.
 * Please check the locking methods to see details about the behaviour.
 * <p>
 * Distributed locks are unfortunately NOT EQUIVALENT to single-node mutexes
 * because of the complexities in distributed systems, such as uncertain
 * communication patterns, and independent and partial failures.
 * In an asynchronous network, no lock service can guarantee mutual exclusion,
 * because there is no way to distinguish between a slow and a crashed process.
 * Consider the following scenario, where a Hazelcast client acquires
 * a {@link FencedLock}, then hits a long GC pause. Since it will not be able
 * to commit session heartbeats while paused, its CP session will be eventually
 * closed. After this moment, another Hazelcast client can acquire this lock.
 * If the first client wakes up again, it may not immediately notice that it
 * has lost ownership of the lock. In this case, multiple clients think they
 * hold the lock. If they attempt to perform an operation on a shared resource,
 * they can break the system. To prevent such situations, you can choose to use
 * an infinite session timeout, but this time probably you are going to deal
 * with liveliness issues. For the scenario above, even if the first client
 * actually crashes, requests sent by 2 clients can be re-ordered in the network
 * and hit the external resource in reverse order.
 * <p>
 * There is a simple solution for this problem. Lock holders are ordered by a
 * monotonic fencing token, which increments each time the lock is assigned to
 * a new owner. This fencing token can be passed to external services or
 * resources to ensure sequential execution of side effects performed by lock
 * holders.
 * <p>
 * The following figure illustrates the idea. Client-1 acquires the lock first
 * and receives 1 as its fencing token. Then, it passes this token to the
 * external service, which is our shared resource in this scenario.
 * Just after that, Client-1 hits a long GC pause and eventually loses
 * ownership of the lock because it misses to commit CP session heartbeats.
 * Then, Client-2 chimes in and acquires the lock. Similar to Client-1,
 * Client-2 passes its fencing token to the external service. After that,
 * once Client-1 comes back alive, its write request will be rejected
 * by the external service, and only Client-2 will be able to safely talk it.
 * <pre>
 *                                                       CLIENT-1's session is expired.
 *                                                                    |
 * |------------------|               LOCK is acquired by CLIENT-1.   |     LOCK is acquired by CLIENT-2.
 * |       LOCK       | . . . . . . . - - - - - - - - - - - - - - - - | . . + + + + + + + + + + + + + + + + + + + + + + + + + + +
 * |------------------|             /\ \ fence = 1                    |   /| \ fence = 2
 *                                 /    \                                /    \
 * |------------------|           /      \       |                      /      \         | CLIENT-1 wakes up.
 * |     CLIENT-1     | . . . . ./. . . . \/. . .|_ _ _ _ _ _ _ _ _ _  /_ _ _ _ \ _ _ _ _|. . . . . . . . . . . . . . . . . . . .
 * |------------------|    lock()            \    CLIENT-1 is paused. /          \    write(A) \
 *                               set_fence(1) \                      /            \             \
 * |------------------|                        \                    /              \             \
 * |     CLIENT-2     | . . . . . . . . . . . . \ . . . . . . . . ./. . . . . . . . \/. . . . . . \ . . . . . . . . . . . . . . .
 * |------------------|                          \           lock()                    \           \      write(B) \
 *                                                \                        set_fence(2) \           \               \
 * |------------------|                            \   |                                 \   |       \               \
 * | EXTERNAL SERVICE | . . . . . . . . . . . . . . \/ |- - - - - - - - - - - - - - - - - \/ |+ + + + \/  + + + + + + \/  + + + +
 * |------------------|                                |                                     | write(A) fails.    write(B) ok.
 *                                                     | SERVICE belongs to CLIENT-1.        | SERVICE belongs to CLIENT-2.
 * </pre>
 * You can read more about the fencing token idea in Martin Kleppmann's
 * "How to do distributed locking" blog post and Google's Chubby paper.
 * {@link FencedLock} integrates this idea with the {@link Lock}
 * abstraction.
 * <p>
 * All of the API methods in the new {@link FencedLock} abstraction offer
 * exactly-once execution semantics. For instance, even if a {@link #lock()}
 * call is internally retried because of a crashed CP member, the lock is
 * acquired only once. The same rule also applies to the other methods
 * in the API.
 *
 * @see FencedLockConfig
 * @see CPSessionManagementService
 * @see CPSession
 * @see LockOwnershipLostException
 * @see LockAcquireLimitReachedException
 */
public interface FencedLock extends Lock, DistributedObject {

    /**
     * Representation of a failed lock attempt where
     * the caller thread has not acquired the lock
     */
    long INVALID_FENCE = 0L;

    /**
     * Acquires the lock.
     * <p>
     * When the caller already holds the lock and the current lock() call is
     * reentrant, the call can fail with
     * {@link LockAcquireLimitReachedException} if the lock acquire limit is
     * already reached. Please see {@link FencedLockConfig} for more
     * information.
     * <p>
     * If the lock is not available then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock has been
     * acquired.
     * <p>
     * Consider the following scenario:
     * <pre>
     *     FencedLock lock = ...;
     *     lock.lock();
     *     // JVM of the caller thread hits a long pause
     *     // and its CP session is closed on the CP group.
     *     lock.lock();
     * </pre>
     * In this scenario, a thread acquires the lock, then its JVM instance
     * encounters a long pause, which is longer than
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. In this case,
     * its CP session will be closed on the corresponding CP group because
     * it could not commit session heartbeats in the meantime. After the JVM
     * instance wakes up again, the same thread attempts to acquire the lock
     * reentrantly. In this case, the second lock() call fails by throwing
     * {@link LockOwnershipLostException} which extends
     * {@link IllegalMonitorStateException}. If the caller wants to deal with
     * its session loss by taking some custom actions, it can handle the thrown
     * {@link LockOwnershipLostException} instance. Otherwise, it can treat it
     * as a regular {@link IllegalMonitorStateException}.
     *
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while locking reentrantly
     * @throws LockAcquireLimitReachedException if the lock call is reentrant
     *         and the configured lock acquire limit is already reached.
     */
    void lock();

    /**
     * Acquires the lock unless the current thread is
     * {@linkplain Thread#interrupt interrupted}.
     * <p>
     * When the caller already holds the lock and the current lock() call is
     * reentrant, the call can fail with
     * {@link LockAcquireLimitReachedException} if the lock acquire limit is
     * already reached. Please see {@link FencedLockConfig} for more
     * information.
     * <p>
     * If the lock is not available then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock has been
     * acquired. Interruption may not be possible after the lock request
     * arrives to the CP group, if the proxy does not attempt to retry its
     * lock request because of a failure in the system.
     * <p>
     * Please note that even if {@link InterruptedException} is thrown,
     * the lock may be acquired on the CP group.
     * <p>
     * When {@link InterruptedException} is thrown, the current thread's
     * interrupted status is cleared.
     * <p>
     * Consider the following scenario:
     * <pre>
     *     FencedLock lock = ...;
     *     lock.lockInterruptibly();
     *     // JVM of the caller thread hits a long pause
     *     // and its CP session is closed on the CP group.
     *     lock.lockInterruptibly();
     * </pre>
     * In this scenario, a thread acquires the lock, then its JVM instance
     * encounters a long pause, which is longer than
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. In this case,
     * its CP session will be closed on the corresponding CP group because
     * it could not commit session heartbeats in the meantime. After the JVM
     * instance wakes up again, the same thread attempts to acquire the lock
     * reentrantly. In this case, the second lock() call fails by throwing
     * {@link LockOwnershipLostException} which extends
     * {@link IllegalMonitorStateException}. If the caller wants to deal with
     * its session loss by taking some custom actions, it can handle the thrown
     * {@link LockOwnershipLostException} instance. Otherwise, it can treat it
     * as a regular {@link IllegalMonitorStateException}.
     *
     * @throws InterruptedException if the current thread is interrupted while
     *         acquiring the lock.
     *
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while locking reentrantly
     * @throws LockAcquireLimitReachedException if the lock call is reentrant
     *         and the configured lock acquire limit is already reached.
     */
    void lockInterruptibly() throws InterruptedException;

    /**
     * Acquires the lock and returns the fencing token assigned to the current
     * thread for this lock acquire. If the lock is acquired reentrantly,
     * the same fencing token is returned, or the lock() call can fail with
     * {@link LockAcquireLimitReachedException} if the lock acquire limit is
     * already reached. Please see {@link FencedLockConfig} for more
     * information.
     * <p>
     * If the lock is not available then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock has been
     * acquired.
     * <p>
     * This is a convenience method for the following pattern:
     * <pre>
     *     FencedLock lock = ...;
     *     lock.lock();
     *     return lock.getFence();
     * </pre>
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *     FencedLock lock = ...; // the lock is free
     *     lock.lockAndGetFence();
     *     // JVM of the caller thread hits a long pause
     *     // and its CP session is closed on the CP group.
     *     lock.lockAndGetFence();
     * </pre>
     * In this scenario, a thread acquires the lock, then its JVM instance
     * encounters a long pause, which is longer than
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. In this case,
     * its CP session will be closed on the corresponding CP group because
     * it could not commit session heartbeats in the meantime. After the JVM
     * instance wakes up again, the same thread attempts to acquire the lock
     * reentrantly. In this case, the second lock() call fails by throwing
     * {@link LockOwnershipLostException} which extends
     * {@link IllegalMonitorStateException}. If the caller wants to deal with
     * its session loss by taking some custom actions, it can handle the thrown
     * {@link LockOwnershipLostException} instance. Otherwise, it can treat it
     * as a regular {@link IllegalMonitorStateException}.
     * <p>
     * Fencing tokens are monotonic numbers that are incremented each time
     * the lock switches from the free state to the acquired state. They are
     * simply used for ordering lock holders. A lock holder can pass
     * its fencing to the shared resource to fence off previous lock holders.
     * When this resource receives an operation, it can validate the fencing
     * token in the operation.
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *     FencedLock lock = ...; // the lock is free
     *     long fence1 = lock.lockAndGetFence(); // (1)
     *     long fence2 = lock.lockAndGetFence(); // (2)
     *     assert fence1 == fence2;
     *     lock.unlock();
     *     lock.unlock();
     *     long fence3 = lock.lockAndGetFence(); // (3)
     *     assert fence3 &gt; fence1;
     * </pre>
     * In this scenario, the lock is acquired by a thread in the cluster. Then,
     * the same thread reentrantly acquires the lock again. The fencing token
     * returned from the second acquire is equal to the one returned from the
     * first acquire, because of reentrancy. After the second acquire, the lock
     * is released 2 times, hence becomes free. There is a third lock acquire
     * here, which returns a new fencing token. Because this last lock acquire
     * is not reentrant, its fencing token is guaranteed to be larger than the
     * previous tokens, independent of the thread that has acquired the lock.
     *
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while locking reentrantly
     * @throws LockAcquireLimitReachedException if the lock call is reentrant
     *         and the configured lock acquire limit is already reached.
     */
    long lockAndGetFence();

    /**
     * Acquires the lock if it is available or already held by the current
     * thread at the time of invocation &amp; the acquire limit is not exceeded,
     * and immediately returns with the value {@code true}. If the lock is not
     * available, then this method immediately returns with the value
     * {@code false}. When the call is reentrant, it can return {@code false}
     * if the lock acquire limit is exceeded. Please see
     * {@link FencedLockConfig} for more information.
     * <p>
     * A typical usage idiom for this method would be:
     * <pre>
     *     FencedLock lock = ...;
     *     if (lock.tryLock()) {
     *         try {
     *             // manipulate protected state
     *         } finally {
     *             lock.unlock();
     *         }
     *     } else {
     *         // perform alternative actions
     *     }
     * </pre>
     * This usage ensures that the lock is unlocked if it was acquired,
     * and doesn't try to unlock if the lock was not acquired.
     *
     * @return {@code true} if the lock was acquired and
     *         {@code false} otherwise
     *
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while locking reentrantly
     */
    boolean tryLock();

    /**
     * Acquires the lock only if it is free or already held by the current
     * thread at the time of invocation &amp; the acquire limit is not exceeded,
     * and returns the fencing token assigned to the current thread for this
     * lock acquire. If the lock is acquired reentrantly, the same fencing
     * token is returned. If the lock is already held by another caller or
     * the lock acquire limit is exceeded, then this method immediately returns
     * {@link #INVALID_FENCE} that represents a failed lock attempt.
     * Please see {@link FencedLockConfig} for more information.
     * <p>
     * This is a convenience method for the following pattern:
     * <pre>
     *     FencedLock lock = ...;
     *     if (lock.tryLock()) {
     *         return lock.getFence();
     *     } else {
     *         return FencedLock.INVALID_FENCE;
     *     }
     * </pre>
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *     FencedLock lock = ...; // the lock is free
     *     lock.tryLockAndGetFence();
     *     // JVM of the caller thread hits a long pause
     *     // and its CP session is closed on the CP group.
     *     lock.tryLockAndGetFence();
     * </pre>
     * In this scenario, a thread acquires the lock, then its JVM instance
     * encounters a long pause, which is longer than
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. In this case,
     * its CP session will be closed on the corresponding CP group because
     * it could not commit session heartbeats in the meantime. After the JVM
     * instance wakes up again, the same thread attempts to acquire the lock
     * reentrantly. In this case, the second lock() call fails by throwing
     * {@link LockOwnershipLostException} which extends
     * {@link IllegalMonitorStateException}. If the caller wants to deal with
     * its session loss by taking some custom actions, it can handle the thrown
     * {@link LockOwnershipLostException} instance. Otherwise, it can treat it
     * as a regular {@link IllegalMonitorStateException}.
     * <p>
     * Fencing tokens are monotonic numbers that are incremented each time
     * the lock switches from the free state to the acquired state. They are
     * simply used for ordering lock holders. A lock holder can pass
     * its fencing to the shared resource to fence off previous lock holders.
     * When this resource receives an operation, it can validate the fencing
     * token in the operation.
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *     FencedLock lock = ...; // the lock is free
     *     long fence1 = lock.tryLockAndGetFence(); // (1)
     *     long fence2 = lock.tryLockAndGetFence(); // (2)
     *     assert fence1 == fence2;
     *     lock.unlock();
     *     lock.unlock();
     *     long fence3 = lock.tryLockAndGetFence(); // (3)
     *     assert fence3 &gt; fence1;
     * </pre>
     * In this scenario, the lock is acquired by a thread in the cluster. Then,
     * the same thread reentrantly acquires the lock again. The fencing token
     * returned from the second acquire is equal to the one returned from the
     * first acquire, because of reentrancy. After the second acquire, the lock
     * is released 2 times, hence becomes free. There is a third lock acquire
     * here, which returns a new fencing token. Because this last lock acquire
     * is not reentrant, its fencing token is guaranteed to be larger than the
     * previous tokens, independent of the thread that has acquired the lock.
     *
     * @return the fencing token if the lock was acquired and
     *         {@link #INVALID_FENCE} otherwise
     *
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while locking reentrantly
     */
    long tryLockAndGetFence();

    /**
     * Acquires the lock if it is free within the given waiting time,
     * or already held by the current thread.
     * <p>
     * If the lock is available, this method returns immediately with the value
     * {@code true}. When the call is reentrant, it immediately returns
     * {@code true} if the lock acquire limit is not exceeded. Otherwise,
     * it returns {@code false} on the reentrant lock attempt if the acquire
     * limit is exceeded. Please see {@link FencedLockConfig} for more
     * information.
     * <p>
     * If the lock is not available then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock is
     * acquired by the current thread or the specified waiting time elapses.
     * <p>
     * If the lock is acquired, then the value {@code true} is returned.
     * <p>
     * If the specified waiting time elapses, then the value {@code false}
     * is returned. If the time is less than or equal to zero, the method does
     * not wait at all.
     *
     * @param time the maximum time to wait for the lock
     * @param unit the time unit of the {@code time} argument
     * @return {@code true} if the lock was acquired and {@code false}
     *         if the waiting time elapsed before the lock was acquired
     *
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while locking reentrantly
     */
    boolean tryLock(long time, TimeUnit unit);

    /**
     * Acquires the lock if it is free within the given waiting time,
     * or already held by the current thread at the time of invocation &amp;
     * the acquire limit is not exceeded, and returns the fencing token
     * assigned to the current thread for this lock acquire. If the lock is
     * acquired reentrantly, the same fencing token is returned. If the lock
     * acquire limit is exceeded, then this method immediately returns
     * {@link #INVALID_FENCE} that represents a failed lock attempt.
     * Please see {@link FencedLockConfig} for more information.
     * <p>
     * If the lock is not available then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock is
     * acquired by the current thread or the specified waiting time elapses.
     * <p>
     * If the specified waiting time elapses, then {@link #INVALID_FENCE}
     * is returned. If the time is less than or equal to zero, the method does
     * not wait at all.
     * <p>
     * This is a convenience method for the following pattern:
     * <pre>
     *     FencedLock lock = ...;
     *     if (lock.tryLock(time, unit)) {
     *         return lock.getFence();
     *     } else {
     *         return FencedLock.INVALID_FENCE;
     *     }
     * </pre>
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *      FencedLock lock = ...; // the lock is free
     *      lock.tryLockAndGetFence(time, unit);
     *      // JVM of the caller thread hits a long pause and its CP session
     *      is closed on the CP group.
     *      lock.tryLockAndGetFence(time, unit);
     * </pre>
     * In this scenario, a thread acquires the lock, then its JVM instance
     * encounters a long pause, which is longer than
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. In this case,
     * its CP session will be closed on the corresponding CP group because
     * it could not commit session heartbeats in the meantime. After the JVM
     * instance wakes up again, the same thread attempts to acquire the lock
     * reentrantly. In this case, the second lock() call fails by throwing
     * {@link LockOwnershipLostException} which extends
     * {@link IllegalMonitorStateException}. If the caller wants to deal with
     * its session loss by taking some custom actions, it can handle the thrown
     * {@link LockOwnershipLostException} instance. Otherwise, it can treat it
     * as a regular {@link IllegalMonitorStateException}.
     * <p>
     * Fencing tokens are monotonic numbers that are incremented each time
     * the lock switches from the free state to the acquired state. They are
     * simply used for ordering lock holders. A lock holder can pass
     * its fencing to the shared resource to fence off previous lock holders.
     * When this resource receives an operation, it can validate the fencing
     * token in the operation.
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *     FencedLock lock = ...; // the lock is free
     *     long fence1 = lock.tryLockAndGetFence(time, unit); // (1)
     *     long fence2 = lock.tryLockAndGetFence(time, unit); // (2)
     *     assert fence1 == fence2;
     *     lock.unlock();
     *     lock.unlock();
     *     long fence3 = lock.tryLockAndGetFence(time, unit); // (3)
     *     assert fence3 &gt; fence1;
     * </pre>
     * In this scenario, the lock is acquired by a thread in the cluster. Then,
     * the same thread reentrantly acquires the lock again. The fencing token
     * returned from the second acquire is equal to the one returned from the
     * first acquire, because of reentrancy. After the second acquire, the lock
     * is released 2 times, hence becomes free. There is a third lock acquire
     * here, which returns a new fencing token. Because this last lock acquire
     * is not reentrant, its fencing token is guaranteed to be larger than the
     * previous tokens, independent of the thread that has acquired the lock.
     *
     * @param time the maximum time to wait for the lock
     * @param unit the time unit of the {@code time} argument
     * @return the fencing token if the lock was acquired and
     *         {@link #INVALID_FENCE} otherwise
     *
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while locking reentrantly
     */
    long tryLockAndGetFence(long time, TimeUnit unit);

    /**
     * Releases the lock if the lock is currently held by the current thread.
     *
     * @throws IllegalMonitorStateException if the lock is not held by
     *         the current thread
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed before the current thread releases the lock
     */
    void unlock();

    /**
     * Returns the fencing token if the lock is held by the current thread.
     * <p>
     * Fencing tokens are monotonic numbers that are incremented each time
     * the lock switches from the free state to the acquired state. They are
     * simply used for ordering lock holders. A lock holder can pass
     * its fencing to the shared resource to fence off previous lock holders.
     * When this resource receives an operation, it can validate the fencing
     * token in the operation.
     *
     * @return the fencing token if the lock is held by the current thread
     *
     * @throws IllegalMonitorStateException if the lock is not held by
     *         the current thread
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while the current thread is holding the lock
     */
    long getFence();

    /**
     * Returns whether this lock is locked or not.
     *
     * @return {@code true} if this lock is locked by any thread
     *         in the cluster, {@code false} otherwise.
     *
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while the current thread is holding the lock
     */
    boolean isLocked();

    /**
     * Returns whether the lock is held by the current thread or not.
     *
     * @return {@code true} if the lock is held by the current thread or not,
     *         {@code false} otherwise.
     *
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while the current thread is holding the lock
     */
    boolean isLockedByCurrentThread();

    /**
     * Returns the reentrant lock count if the lock is held by any thread
     * in the cluster.
     *
     * @return the reentrant lock count if the lock is held by any thread
     *         in the cluster
     *
     * @throws LockOwnershipLostException if the underlying CP session is
     *         closed while the current thread is holding the lock
     */
    int getLockCount();

    /**
     * Returns id of the CP group that runs this {@link FencedLock} instance
     *
     * @return id of the CP group that runs this {@link FencedLock} instance
     */
    CPGroupId getGroupId();

    /**
     * NOT IMPLEMENTED. Fails by throwing {@link UnsupportedOperationException}.
     * <p>
     * May the force be the one who dares to implement
     * a linearizable distributed {@link Condition} :)
     *
     * @throws UnsupportedOperationException for now
     */
    Condition newCondition();
}

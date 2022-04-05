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

package com.hazelcast.cp;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSession;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * ISemaphore is a fault-tolerant distributed alternative to the
 * {@link Semaphore}. Semaphores are often used to restrict the number of
 * threads than can access some physical or logical resource.
 * <p>
 * ISemaphore is a cluster-wide counting semaphore. Conceptually, it maintains
 * a set of permits. Each {@link #acquire()} blocks if necessary until a permit
 * is available, and then takes it. Dually, each {@link #release()} adds a
 * permit, potentially releasing a blocking acquirer. However, no actual permit
 * objects are used; the semaphore just keeps a count of the number available
 * and acts accordingly.
 * <p>
 * Hazelcast's distributed semaphore implementation guarantees that threads
 * invoking any of the {@link #acquire() acquire} methods are selected to
 * obtain permits in the order of their invocations (first-in-first-out; FIFO).
 * Note that FIFO ordering implies the order which the primary replica of an
 * {@link ISemaphore} receives these acquire requests. Therefore, it is
 * possible for one member to invoke {@code acquire} before another member,
 * but its request hits the primary replica after the other member.
 * <p>
 * This class also provides convenience methods, such as
 * {@link #acquire(int) acquire} and {@link #release(int) release}, to work
 * with multiple permits at once. Beware of the increased risk of
 * indefinite postponement when using the multiple-permit acquire. If permits
 * are released one by one, a thread waiting for one permit will acquire
 * it before a thread waiting for multiple permits regardless of the call order.
 * <p>
 * Correct usage of a semaphore is established by programming convention
 * in the application.
 * <p>
 * {@link ISemaphore} is accessed via {@link CPSubsystem#getSemaphore(String)}.
 * It works on top of the Raft consensus algorithm. It offers linearizability during crash
 * failures and network partitions. It is CP with respect to the CAP principle.
 * If a network partition occurs, it remains available on at most one side of
 * the partition.
 * <p>
 * It has 2 variations:
 * <ul>
 * <li>
 * The default impl accessed via {@link CPSubsystem} is session-aware. In this
 * one, when a caller makes its very first {@link #acquire()} call, it starts
 * a new CP session with the underlying CP group. Then, liveliness of the
 * caller is tracked via this CP session. When the caller fails, permits
 * acquired by this HazelcastInstance are automatically and safely released.
 * However, the session-aware version comes with a limitation, that is,
 * a HazelcastInstance cannot release permits before acquiring them
 * first. In other words, a HazelcastInstance can release only
 * the permits it has acquired earlier. It means, you can acquire a permit
 * from one thread and release it from another thread using the same
 * HazelcastInstance, but not different instances of HazelcastInstance. This
 * behaviour is not compatible with {@link Semaphore#release()}. You can use
 * the session-aware CP {@link ISemaphore} impl by disabling JDK compatibility
 * via {@link SemaphoreConfig#setJDKCompatible(boolean)}. Although
 * the session-aware impl has a minor difference to the JDK Semaphore, we think
 * it is a better fit for distributed environments because of its safe
 * auto-cleanup mechanism for acquired permits. Please see
 * {@link CPSession} for the rationale behind the session mechanism.
 * </li>
 * <li>
 * The second impl offered by {@link CPSubsystem} is sessionless. This impl
 * does not perform auto-cleanup of acquired permits on failures. Acquired
 * permits are not bound to HazelcastInstance and permits can be released without
 * acquiring first. This one is compatible with {@link Semaphore#release()}.
 * However, you need to handle failed permit owners on your own. If a Hazelcast
 * server or a client fails while holding some permits, they will not be
 * automatically released. You can use the sessionless CP {@link ISemaphore}
 * impl by enabling JDK compatibility via
 * {@link SemaphoreConfig#setJDKCompatible(boolean)}.
 * </li>
 * </ul>
 * <p>
 * There is a subtle difference between the lock and semaphore abstractions.
 * A lock can be assigned to at most one endpoint at a time, so we have a total
 * order among its holders. However, permits of a semaphore can be assigned to
 * multiple endpoints at a time, which implies that we may not have a total
 * order among permit holders. In fact, permit holders are partially ordered.
 * For this reason, the fencing token approach, which is explained in
 * {@link FencedLock}, does not work for the semaphore abstraction. Moreover,
 * each permit is an independent entity. Multiple permit acquires and reentrant
 * lock acquires of a single endpoint are not equivalent. The only case where
 * a semaphore behaves like a lock is the binary case, where the semaphore has
 * only 1 permit. In this case, the semaphore works like a non-reentrant lock.
 * <p>
 * All of the API methods in the new CP {@link ISemaphore} impl offer
 * the exactly-once execution semantics for the session-aware version.
 * For instance, even if a {@link #release()} call is internally retried
 * because of a crashed Hazelcast member, the permit is released only once.
 * However, this guarantee is not given for the sessionless, a.k.a,
 * JDK-compatible CP {@link ISemaphore}. For this version, you can tune
 * execution semantics via
 * {@link CPSubsystemConfig#setFailOnIndeterminateOperationState(boolean)}.
 */

public interface ISemaphore extends DistributedObject {
    /**
     * Returns the name of this ISemaphore instance.
     *
     * @return name of this instance
     */
    String getName();

    /**
     * Tries to initialize this ISemaphore instance with the given permit count
     *
     * @param permits the given permit count
     * @return true if initialization success. false if already initialized
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    boolean init(int permits);

    /**
     * Acquires a permit if one is available, and returns immediately,
     * reducing the number of available permits by one.
     * <p>
     * If no permit is available, then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until
     * one of three things happens:
     * <ul>
     * <li>some other thread invokes one of the {@link #release} methods for this
     * semaphore and the current thread is next to be assigned a permit,</li>
     * <li>this ISemaphore instance is destroyed, or</li>
     * <li>some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread.</li>
     * </ul>
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or</li>
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting for a permit,</li>
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @throws InterruptedException  if the current thread is interrupted
     * @throws IllegalStateException if hazelcast instance is shutdown while waiting
     */
    void acquire() throws InterruptedException;

    /**
     * Acquires the given number of permits if they are available,
     * and returns immediately, reducing the number of available permits
     * by the given amount.
     * <p>
     * If insufficient permits are available then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until
     * one of three things happens:
     * <ul>
     * <li>some other thread invokes one of the {@link #release() release}
     * methods for this semaphore, the current thread is next to be assigned
     * permits and the number of available permits satisfies this request,</li>
     * <li>or this ISemaphore instance is destroyed,</li>
     * <li>or some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread.</li>
     * </ul>
     * If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method, or</li>
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting for a permit,</li>
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @param permits the number of permits to acquire
     * @throws InterruptedException     if the current thread is interrupted
     * @throws IllegalArgumentException if {@code permits} is negative or zero
     * @throws IllegalStateException    if hazelcast instance is shutdown while waiting
     */
    void acquire(int permits) throws InterruptedException;

    /**
     * Returns the current number of permits currently available in this semaphore.
     * <p>
     * This method is typically used for debugging and testing purposes.
     *
     * @return the number of permits available in this semaphore
     */
    int availablePermits();

    /**
     * Acquires and returns all permits that are available at invocation time.
     *
     * @return the number of permits drained
     */
    int drainPermits();

    /**
     * Reduces the number of available permits by the indicated amount. This
     * method differs from {@code acquire} as it does not block until permits
     * become available. Similarly, if the caller has acquired some permits,
     * they are not released with this call.
     *
     * @param reduction the number of permits to reduce
     * @throws IllegalArgumentException if {@code reduction} is negative
     */
    void reducePermits(int reduction);

    /**
     * Increases the number of available permits by the indicated amount. If
     * there are some threads waiting for permits to become available, they
     * will be notified. Moreover, if the caller has acquired some permits,
     * they are not released with this call.
     *
     * @param increase the number of permits to increase
     * @throws IllegalArgumentException if {@code increase} is negative
     */
    void increasePermits(int increase);

    /**
     * Releases a permit and increases the number of available permits by one.
     * If some threads in the cluster are blocked for acquiring a permit, one
     * of them will unblock by acquiring the permit released by this call.
     * <p>
     * If the underlying {@link ISemaphore} is configured as non-JDK compatible
     * via {@link SemaphoreConfig} then a HazelcastInstance can only release a permit which
     * it has acquired before. In other words, a HazelcastInstance cannot release a permit
     * without acquiring it first.
     * <p>
     * Otherwise, which means the underlying impl is the JDK compatible
     * Semaphore is configured via {@link SemaphoreConfig}, there is no requirement
     * that a HazelcastInstance that releases a permit must have acquired that permit by
     * calling one of the {@link #acquire()} methods. A HazelcastInstance can freely
     * release a permit without acquiring it first. In this case, correct usage
     * of a semaphore is established by programming convention in the application.
     *
     * @throws IllegalStateException if the Semaphore is non-JDK-compatible
     *         and the caller does not have a permit
     */
    void release();

    /**
     * Releases the given number of permits and increases the number of
     * available permits by that amount. If some threads in the cluster are
     * blocked for acquiring permits, they will be notified.
     * <p>
     * If the underlying {@link ISemaphore} impl is the non-JDK compatible
     * CP impl that is configured via {@link SemaphoreConfig} and fetched
     * via {@link CPSubsystem}, then a HazelcastInstance can only release a permit which
     * it has acquired before. In other words, a HazelcastInstance cannot release a permit
     * without acquiring it first.
     * <p>
     * Otherwise, which means the underlying impl is the JDK compatible
     * Semaphore is configured via {@link SemaphoreConfig}, there is no requirement
     * that a HazelcastInstance that releases a permit must have acquired that permit by
     * calling one of the {@link #acquire()} methods. A HazelcastInstance can freely
     * release a permit without acquiring it first. In this case, correct usage
     * of a semaphore is established by programming convention in the application.
     *
     * @param permits the number of permits to release
     * @throws IllegalArgumentException if {@code permits} is negative or zero
     * @throws IllegalStateException if the Semaphore is non-JDK-compatible
     *         and the caller does not have a permit
     */
    void release(int permits);

    /**
     * Acquires a permit if one is available, and returns {@code true}
     * immediately. If no permit is available, returns {@code false}
     * immediately.
     *
     * @return {@code true} if a permit was acquired, {@code false} otherwise
     */
    boolean tryAcquire();

    /**
     * Acquires the given number of permits if they are available, and
     * returns {@code true} immediately. If the requested number of permits are
     * not available, returns {@code false} immediately.
     *
     * @param permits the number of permits to acquire
     * @return {@code true} if the permits were acquired, {@code false} otherwise
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    boolean tryAcquire(int permits);

    /**
     * Acquires a permit and returns {@code true}, if one becomes available
     * during the given waiting time and the current thread has not been
     * {@linkplain Thread#interrupt interrupted}. If a permit is acquired,
     * the number of available permits in the {@link ISemaphore} instance is
     * also reduced by one.
     * <p>
     * If no permit is available, then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until
     * one of three things happens:
     * <ul>
     * <li>some other thread releases a permit and the current thread is next
     * to be assigned a permit,</li>
     * <li>or this ISemaphore instance is destroyed,</li>
     * <li>or some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread,</li>
     * <li>or the specified waiting time elapses.</li>
     * </ul>
     * <p>
     * Returns {@code true} if a permit is successfully acquired.
     * <p>
     * Returns {@code false} if the specified waiting time elapses without
     * acquiring a permit. If the time is less than or equal to zero,
     * the method will not wait at all.
     * <p>
     * If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method,</li>
     * <li>or is {@linkplain Thread#interrupt interrupted} while waiting for
     * a permit,</li>
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @param timeout the maximum time to wait for a permit
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if a permit was acquired and {@code false}
     * if the waiting time elapsed before a permit was acquired
     * @throws InterruptedException  if the current thread is interrupted
     * @throws IllegalStateException if hazelcast instance is shutdown while waiting
     */
    boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Acquires the given number of permits and returns {@code true}, if they
     * become available during the given waiting time. If permits are acquired,
     * the number of available permits in the {@link ISemaphore} instance is
     * also reduced by the given amount.
     * <p>
     * If no sufficient permits are available, then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until one of
     * three things happens:
     * <ul>
     * <li>permits are released by other threads, the current thread is next to
     * be assigned permits and the number of available permits satisfies this
     * request,</li>
     * <li>or some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread,</li>
     * <li>or the specified waiting time elapses.</li>
     * </ul>
     * Returns {@code true} if requested permits are successfully acquired.
     * <p>
     * Returns {@code false} if the specified waiting time elapses without
     * acquiring permits. If the time is less than or equal to zero,
     * the method will not wait at all.
     * <p>
     * If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method,</li>
     * <li>or is {@linkplain Thread#interrupt interrupted} while waiting for
     * a permit,</li>
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @param permits the number of permits to acquire
     * @param timeout the maximum time to wait for the permits
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if all permits were acquired,
     * {@code false} if the waiting time elapsed before all permits could be acquired
     * @throws InterruptedException     if the current thread is interrupted
     * @throws IllegalArgumentException if {@code permits} is negative or zero
     * @throws IllegalStateException    if hazelcast instance is shutdown while waiting
     */
    boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException;
}

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

/**
 * ISemaphore is a backed-up distributed alternative to the {@link java.util.concurrent.Semaphore}.
 * <p/>
 * ISemaphore is a cluster-wide counting semaphore.  Conceptually,
 * it maintains a set of permits.  Each {@link #acquire()} blocks if necessary until
 * a permit is available, and then takes it.  Each {@link #release()} adds a permit,
 * potentially releasing a blocking acquirer. However, no actual permit objects are
 * used; the semaphore just keeps a count of the number available and acts accordingly.
 * <p/>The Hazelcast distributed semaphore implementation guarantees that
 * threads invoking any of the {@link #acquire() acquire} methods are selected
 * to obtain permits in the order in which their invocation of those methods
 * was processed(first-in-first-out; FIFO).  Note that FIFO ordering necessarily
 * applies to specific internal points of execution within the cluster. Therefore,
 * it is possible for one member to invoke {@code acquire} before another, but reach
 * the ordering point after the other, and similarly upon return from the method.
 * <p/>This class also provides convenience methods to {@link
 * #acquire(int) acquire} and {@link #release(int) release} multiple
 * permits at a time.  Beware of the increased risk of indefinite
 * postponement when using the multiple acquire.  If a single permit is
 * released to a semaphore that is currently blocking, a thread waiting
 * for one permit will acquire it before a thread waiting for multiple
 * permits regardless of the call order.
 * <p/>
 * <ul>
 * <li>Correct usage of a semaphore is established by programming convention in the application.
 * </ul>
 *
 */

public interface ISemaphore extends DistributedObject {
    /**
     * Returns the name of this ISemaphore instance.
     *
     * @return name of this instance
     */
    String getName();

    /**
     * Try to initialize this ISemaphore instance with the given permit count
     *
     * @param permits the given permit count
     * @return true if initialization success
     */
    boolean init(int permits);

    /**
     * <p>Acquires a permit if one is available, and returns immediately,
     * reducing the number of available permits by one.
     * <p/>
     * <p>If no permit is available, then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until
     * one of three things happens:
     * <ul>
     * <li>some other thread invokes one of the {@link #release} methods for this
     * semaphore and the current thread is next to be assigned a permit,
     * <li>this ISemaphore instance is destroyed, or
     * <li>some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread.
     * </ul>
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * for a permit,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalStateException      if hazelcast instance is shutdown while waiting
     */
    void acquire() throws InterruptedException;

    /**
     * <p>Acquires the given number of permits if they are available,
     * and returns immediately, reducing the number of available permits
     * by the given amount.
     * <p/>
     * <p>If insufficient permits are available then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until
     * one of three things happens:
     * <ul>
     * <li>some other thread invokes one of the {@link #release() release}
     * methods for this semaphore, the current thread is next to be assigned
     * permits and the number of available permits satisfies this request,
     * <li>this ISemaphore instance is destroyed, or
     * <li>some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread.
     * </ul>
     * <p/>
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method, or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * for a permit,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @param permits the number of permits to acquire
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalArgumentException   if {@code permits} is negative
     * @throws IllegalStateException      if hazelcast instance is shutdown while waiting
     */
    void acquire(int permits) throws InterruptedException;


    /**
     * Returns the current number of permits currently available in this semaphore.
     * <p/>
     * <ul><li>This method is typically used for debugging and testing purposes.
     * </ul>
     *
     * @return the number of permits available in this semaphore
     */
    int availablePermits();


    /**
     * Acquires and returns all permits that are immediately available.
     *
     * @return the number of permits drained
     */
    int drainPermits();

    /**
     * Shrinks the number of available permits by the indicated
     * reduction. This method differs from {@code acquire} in that it does not
     * block waiting for permits to become available.
     *
     * @param reduction the number of permits to remove
     * @throws IllegalArgumentException if {@code reduction} is negative
     */
    void reducePermits(int reduction);

    /**
     * Releases a permit, increasing the number of available permits by
     * one.  If any threads in the cluster are trying to acquire a permit,
     * then one is selected and given the permit that was just released.
     * <p/>
     * There is no requirement that a thread that releases a permit must
     * have acquired that permit by calling one of the {@link #acquire() acquire} methods.
     * Correct usage of a semaphore is established by programming convention
     * in the application.
     */
    void release();

    /**
     * Releases the given number of permits, increasing the number of
     * available permits by that amount.
     * <p/>
     * There is no requirement that a thread that releases a permit must
     * have acquired that permit by calling one of the {@link #acquire() acquire} methods.
     * Correct usage of a semaphore is established by programming convention
     * in the application.
     *
     * @param permits the number of permits to release
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    void release(int permits);

    /**
     * Acquires a permit, if one is available and returns immediately,
     * with the value {@code true},
     * reducing the number of available permits by one.
     * <p/>
     * If no permit is available then this method will return
     * immediately with the value {@code false}.
     *
     * @return {@code true} if a permit was acquired and {@code false}
     *         otherwise
     */
    boolean tryAcquire();

    /**
     * Acquires the given number of permits, if they are available, and
     * returns immediately, with the value {@code true},
     * reducing the number of available permits by the given amount.
     * <p/>
     * <p>If insufficient permits are available then this method will return
     * immediately with the value {@code false} and the number of available
     * permits is unchanged.
     *
     * @param permits the number of permits to acquire
     * @return {@code true} if the permits were acquired and
     *         {@code false} otherwise
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    boolean tryAcquire(int permits);

    /**
     * Acquires a permit from this semaphore if one becomes available
     * within the given waiting time and the current thread has not
     * been {@linkplain Thread#interrupt interrupted}.
     * <p/>
     * Acquires a permit if one is available and returns immediately
     * with the value {@code true},
     * reducing the number of available permits by one.
     * <p/>
     * If no permit is available, then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until
     * one of three things happens:
     * <ul>
     * <li>some other thread invokes the {@link #release} method for this
     * semaphore and the current thread is next to be assigned a permit, or
     * <li>some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread, or
     * <li>the specified waiting time elapses.
     * </ul>
     * <p/>
     * If a permit is acquired then the value {@code true} is returned.
     * <p/>
     * If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     * <p/>
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * for a permit,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @param timeout the maximum time to wait for a permit
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if a permit was acquired and {@code false}
     *         if the waiting time elapsed before a permit was acquired
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalStateException      if hazelcast instance is shutdown while waiting
     */
    boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Acquires the given number of permits if they are available and
     * returns immediately with the value {@code true},
     * reducing the number of available permits by the given amount.
     * <p/>
     * If insufficient permits are available, then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of three things happens:
     * <ul>
     * <li>some other thread invokes one of the {@link #release() release}
     * methods for this semaphore, the current thread is next to be assigned
     * permits and the number of available permits satisfies this request, or
     * <li>some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread, or
     * <li>the specified waiting time elapses.
     * </ul>
     * <p/>
     * If the permits are acquired then {@code true} is returned.
     * <p/>
     * If the specified waiting time elapses then {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     * <p/>
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method, or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * for a permit,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @param permits the number of permits to acquire
     * @param timeout the maximum time to wait for the permits
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if all permits were acquired, {@code false}
     *         if the waiting time elapsed before all permits could be acquired
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalArgumentException   if {@code permits} is negative
     * @throws IllegalStateException      if hazelcast instance is shutdown while waiting
     */
    boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException;

}

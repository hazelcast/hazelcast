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
 * ICountDownLatch is a backed-up distributed alternative to the
 * {@link java.util.concurrent.CountDownLatch java.util.concurrent.CountDownLatch}.
 * <p/>
 * ICountDownLatch is a cluster-wide synchronization aid
 * that allows one or more threads to wait until a set of operations being
 * performed in other threads completes.
 * <p/>
 * There are a few differences compared to the {@link ICountDownLatch}:
 * <ol>
 *    <li>
 *         the ICountDownLatch count can be reset using {@link #trySetCount(int)} after a countdown
 *         has finished but not during an active count. This allows the same latch instance to be reused.
 *    </li>
 *    <li>
 *         There is no await() method to do an unbound wait since this is undesirable in a distributed
 *         application: for example, a cluster can split or the master and
 *         replicas could all die. In most cases, it is best to configure an explicit timeout so you have the ability
 *         to deal with these situations.
 *    </li>
 * </ol>
 *
 */
public interface ICountDownLatch extends DistributedObject {

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, or an exception is thrown, or the specified waiting time elapses.
     * <p/>
     * <p>If the current count is zero then this method returns immediately
     * with the value {@code true}.
     * <p/>
     * <p>If the current count is greater than zero, then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of five things happen:
     * <ul>
     * <li>the count reaches zero due to invocations of the
     * {@link #countDown} method,
     * <li>this ICountDownLatch instance is destroyed,
     * <li>the countdown owner becomes disconnected,
     * <li>some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread, or
     * <li>the specified waiting time elapses.
     * </ul>
     * <p/>
     * <p>If the count reaches zero, then the method returns with the
     * value {@code true}.
     * <p/>
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method, or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     * <p>If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if the count reached zero, {@code false}
     *         if the waiting time elapsed before the count reached zero
     * @throws InterruptedException if the current thread is interrupted
     * @throws IllegalStateException if the Hazelcast instance is shutdown while waiting
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Decrements the count of the latch, releasing all waiting threads if
     * the count reaches zero.
     * <p/>
     * If the current count is greater than zero, then it is decremented.
     * If the new count is zero:
     * <ul>
     * <li>All waiting threads are re-enabled for thread scheduling purposes, and
     * <li>Countdown owner is set to {@code null}.
     * </ul>
     * <p/>
     * If the current count equals zero, then nothing happens.
     */
    void countDown();

    /**
     * Returns the current count.
     *
     * @return the current count
     */
    int getCount();

    /**
     * Sets the count to the given value if the current count is zero.
     * <p/>If count is not zero, then this method does nothing and returns {@code false}.
     *
     * @param count the number of times {@link #countDown} must be invoked
     *              before threads can pass through {@link #await}
     * @return {@code true} if the new count was set, {@code false} if the current
     *         count is not zero
     * @throws IllegalArgumentException if {@code count} is negative
     */
    boolean trySetCount(int count);
}

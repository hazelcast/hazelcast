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

import com.hazelcast.monitor.LocalCountDownLatchStats;

import java.util.concurrent.TimeUnit;

/**
 * ICountDownLatch is a backed-up distributed implementation of
 * {@link java.util.concurrent.CountDownLatch java.util.concurrent.CountDownLatch}.
 * <p/>
 * Hazelcast's ICountDownLatch is a cluster-wide synchronization aid
 * that allows one or more threads to wait until a set of operations being
 * performed in other threads completes.
 * <p/>
 * Unlike Java's implementation, Hazelcast's ICountDownLatch count can be re-set
 * after a countdown has finished but not during an active count. This allows the same
 * proxy instance to be reused.
 * <p/>The Hazelcast member that successfully invokes {@link #setCount(int)} becomes
 * the owner of the countdown and is responsible for staying connected to
 * the cluster until the count reaches zero.  If the owner becomes disconnected prior
 * to count reaching zero all awaiting threads will be notified.  This provides a
 * safety mechanism in the distributed environment.
 */
public interface ICountDownLatch extends Instance {
    /**
     * Returns the name of this ICountDownLatch instance.
     *
     * @return name of this instance
     */
    String getName();

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero or an exception is thrown.
     * <p/>
     * <p>If the current count is zero then this method returns immediately.
     * <p/>
     * <p>If the current count is greater than zero then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of four things happen:
     * <ul>
     * <li>The count reaches zero due to invocations of the
     * {@link #countDown} method;
     * <li>This ICountDownLatch instance is destroyed;
     * <li>The countdown owner becomes disconnected; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread.
     * </ul>
     * <p/>If the ICountDownLatch instance is destroyed while waiting then
     * {@link InstanceDestroyedException} will be thrown.
     * <p/>If the countdown owner becomes disconnected while waiting then
     * {@link MemberLeftException} will be thrown.
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @throws InstanceDestroyedException if the instance is destroyed while waiting
     * @throws MemberLeftException        if the countdown owner becomes disconnected while waiting
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalStateException      if hazelcast instance is shutdown while waiting
     */
    public void await() throws InstanceDestroyedException, MemberLeftException, InterruptedException;

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, an exception is thrown, or the specified waiting time elapses.
     * <p/>
     * <p>If the current count is zero then this method returns immediately
     * with the value {@code true}.
     * <p/>
     * <p>If the current count is greater than zero then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of five things happen:
     * <ul>
     * <li>The count reaches zero due to invocations of the
     * {@link #countDown} method;
     * <li>This ICountDownLatch instance is destroyed;
     * <li>The countdown owner becomes disconnected;
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     * <li>The specified waiting time elapses.
     * </ul>
     * <p/>
     * <p>If the count reaches zero then the method returns with the
     * value {@code true}.
     * <p/>
     * <p/>If the ICountDownLatch instance is destroyed while waiting then
     * {@link InstanceDestroyedException} will be thrown.
     * <p/>If the countdown owner becomes disconnected while waiting then
     * {@link MemberLeftException} will be thrown.
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
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
     * @return {@code true} if the count reached zero and {@code false}
     *         if the waiting time elapsed before the count reached zero
     * @throws InstanceDestroyedException if the instance is destroyed while waiting
     * @throws MemberLeftException        if the countdown owner becomes disconnected while waiting
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalStateException      if hazelcast instance is shutdown while waiting
     */
    public boolean await(long timeout, TimeUnit unit) throws InstanceDestroyedException, MemberLeftException, InterruptedException;

    /**
     * Decrements the count of the latch, releasing all waiting threads if
     * the count reaches zero.
     * <p/>
     * If the current count is greater than zero then it is decremented.
     * If the new count is zero:
     * <ul>
     * <li>All waiting threads are re-enabled for thread scheduling purposes; and
     * <li>Countdown owner is set to {@code null}.
     * </ul>
     * <p/>
     * If the current count equals zero then nothing happens.
     */
    public void countDown();

    /**
     * Returns whether the current count is greater than zero.
     *
     * @return {@code true} if count is greater than zero
     */
    public boolean hasCount();

    /**
     * Sets the count to the given value if the current count is zero. The calling
     * cluster member becomes the owner of the countdown and is responsible for
     * staying connected to the cluster until the count reaches zero.
     * <p/>If the owner becomes disconnected before the count reaches zero:
     * <ul>
     * <li>Count will be set to zero;
     * <li>Countdown owner will be set to {@code null}; and
     * <li>All awaiting threads will be thrown a {@link MemberLeftException}.
     * </ul>
     * <p/>If count is not zero then this method does nothing and returns {@code false}.
     *
     * @param count the number of times {@link #countDown} must be invoked
     *              before threads can pass through {@link #await}
     * @return {@code true} if the new count was set or {@code false} if the current
     *         count is not zero
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public boolean setCount(int count);

    LocalCountDownLatchStats getLocalCountDownLatchStats();
}

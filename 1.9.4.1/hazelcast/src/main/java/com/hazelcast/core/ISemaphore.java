/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hazelcast.monitor.LocalSemaphoreStats;


/**
 * ISemaphore is a backed-up distributed implementation of {@link java.util.concurrent.Semaphore java.util.concurrent.Semaphore}.
 * <p/>
 * Hazelcast's ISemaphore is a cluster-wide counting semaphore.  Conceptually,
 * it maintains a set of permits.  Each {@link #acquire()} blocks if necessary until
 * a permit is available, and then takes it.  Each {@link #release()} adds a permit,
 * potentially releasing a blocking acquirer. However, no actual permit objects are
 * used; the semaphore just keeps a count of the number available and acts accordingly.
 * <p/>The Hazelcast distributed semaphore implementation guarantees that
 * threads invoking any of the {@link #acquire() acquire} methods are selected
 * to obtain permits in the order in which their invocation of those methods
 * was processed(first-in-first-out; FIFO).  Note that FIFO ordering necessarily
 * applies to specific internal points of execution within the cluster.  So,
 * it is possible for one member to invoke {@code acquire} before another, but reach
 * the ordering point after the other, and similarly upon return from the method.
 * <p/>The Hazelcast semaphore also allows you to {@link #attach()}/{@link #detach()}
 * permits to the caller address.  This provides a safety mechanism in case
 * that address becomes disconnected from the cluster.  Attached permits will
 * automatically be {@link #release() released} to the semaphore if a disconnection
 * occurs.  An address can also have an excess number of detached permits and is
 * represented by a negative attached permit count.  This is the number of permits
 * that the semaphore will automatically be {@link #reducePermits(int) reduced} by
 * if a disconnection occurs.
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
 * @since 1.9.3
 */

public interface ISemaphore extends Instance {
    /**
     * Returns the name of this ISemaphore instance.
     *
     * @return name of this instance
     */
    public String getName();

    /**
     * <p>Acquires a permit, if one is available and returns immediately,
     * reducing the number of available permits by one.
     * <p/>
     * <p>If no permit is available then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until
     * one of three things happens:
     * <ul>
     * <li>Some other thread invokes one of the {@link #release} methods for this
     * semaphore and the current thread is next to be assigned a permit;
     * <li>This ISemaphore instance is destroyed; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread.
     * </ul>
     * <p>If the ISemaphore instance is destroyed while the thread is waiting
     * then {@link InstanceDestroyedException} will be thrown.
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * for a permit,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @throws InstanceDestroyedException if the instance is destroyed while waiting
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalStateException    if hazelcast instance is shutdown while waiting
     */
    public void acquire() throws InstanceDestroyedException, InterruptedException;

    /**
     * <p>Acquires the given number of permits, if they are available,
     * and returns immediately, reducing the number of available permits
     * by the given amount.
     * <p/>
     * <p>If insufficient permits are available then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until
     * one of three things happens:
     * <ul>
     * <li>Some other thread invokes one of the {@link #release() release}
     * methods for this semaphore, the current thread is next to be assigned
     * permits and the number of available permits satisfies this request;
     * <li>This ISemaphore instance is destroyed; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread.
     * </ul>
     * <p/>
     * <p/>If the ISemaphore instance is destroyed while waiting then
     * {@link InstanceDestroyedException} will be thrown.
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * for a permit,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @param permits the number of permits to acquire
     * @throws InstanceDestroyedException if the instance is destroyed while waiting
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalArgumentException   if {@code permits} is negative
     * @throws IllegalStateException    if hazelcast instance is shutdown while waiting
     */
    public void acquire(int permits) throws InstanceDestroyedException, InterruptedException;

    /**
     * Asynchronously acquires a permit.
     * <p/>
     * <pre>
     * Future future = semaphore.acquireAsync();
     * // do some other stuff, when ready get the result
     * Object value = future.get();
     * </pre>
     * Future.get() will block until the actual acquire() completes.
     * If the application requires timely response,
     * then Future.get(timeout, timeunit) can be used.
     * <pre>
     * Future future = semaphore.acquireAsync();
     * try{
     *   Object value = future.get(40, TimeUnit.MILLISECOND);
     * }catch (TimeoutException t) {
     *   // time wasn't enough
     * }
     * </pre>
     * This method itself does not throw any exceptions. Exceptions occur during
     * the {@link java.util.concurrent.Future#get() future.get()} operation.
     * <p/>
     * <p>If insufficient permits are available when calling
     * {@link java.util.concurrent.Future#get() future.get()} then the current
     * thread becomes disabled for thread scheduling purposes and lies dormant until
     * one of four things happens:
     * <ul>
     * <li>Some other thread invokes one of the {@link #release() release}
     * methods for this semaphore, the current thread is next to be assigned
     * permits and the number of available permits satisfies this request;
     * <li>This ISemaphore instance is destroyed;
     * <li>The get method is called with a waiting time that elapses; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread.
     * </ul>
     * <p/>
     * <p/>If the ISemaphore instance is destroyed while the
     * {@link java.util.concurrent.Future#get() future.get()} is waiting then
     * {@link java.util.concurrent.ExecutionException} will be thrown with
     * {@link InstanceDestroyedException} set as it's cause.
     * <p/>If the Hazelcast instance is shutdows while the
     * {@link java.util.concurrent.Future#get() future.get()} is waiting then
     * {@link IllegalStateException} will be thrown.
     * <p/>If when calling {@link java.util.concurrent.Future#get() future.get()}
     * a timeout is specified and the a permit cannot be acquired within the
     * timeout period {@link java.util.concurrent.TimeoutException} will be thrown.
     * <p>If when calling {@link java.util.concurrent.Future#get() future.get()}
     * the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * for a permit,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     * <p/>If the thread {@link java.util.concurrent.Future#get() future.get()} throws an
     * InterruptedException, the acquire request is still outstanding and
     * {@link java.util.concurrent.Future#get() future.get()}
     * may be called again. To cancel the actual acquire,
     * {@link java.util.concurrent.Future#cancel(boolean) future.cancel()}
     * must be called. If the cancel method returns {@code false} then
     * it was too late to cancel the request and the permit was acquired.
     * The {@link java.util.concurrent.Future#cancel(boolean) future.cancel()}
     * <tt>mayInterruptIfRunning</tt> argument is ignored.
     * <p/>
     *
     * @return Future from which the acquire result can be obtained.
     * @see java.util.concurrent.Future
     */
    public Future acquireAsync();

    /**
     * Asynchronously acquires a given number of permits.
     * <p/>
     * See {@link #acquireAsync()}.
     *
     * @param permits the number of permits to acquire
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    public Future acquireAsync(int permits);

    /**
     * Acquires and attaches a permit to the caller's address.
     * <p/>
     * See {@link #acquire()} and {@link #attach()}.
     *
     * @throws IllegalStateException    if hazelcast instance is shutdown while waiting
     */
    public void acquireAttach() throws InstanceDestroyedException, InterruptedException;

    /**
     * Acquires and attaches the given number of permits to the caller's
     * address.
     * <p/>
     * See {@link #acquire()} and {@link #attach()}.
     *
     * @param permits the number of permits to acquire and attach
     * @throws IllegalArgumentException if {@code permits} is negative
     * @throws IllegalStateException    if hazelcast instance is shutdown while waiting
     */
    public void acquireAttach(int permits) throws InstanceDestroyedException, InterruptedException;

    /**
     * Asynchronously acquires and attaches a permit to the caller's address.
     * <p/>
     * See {@link #acquireAsync()} and {@link #attach()}.
     *
     */
    public Future acquireAttachAsync();

    /**
     * Asynchronously acquires and attaches the given number of permits
     * to the caller's address.
     * <p/>
     * See {@link #acquireAsync()} and {@link #attach()}.
     *
     * @param permits the number of permits to acquire and attach
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    public Future acquireAttachAsync(int permits);

    /**
     * Attaches a permit to the caller's address.
     * <p/>
     * See {@link #attachedPermits()}.
     */
    public void attach();

    /**
     * Attaches the given number of permits to the caller's address.
     * <p/>
     * See {@link #attachedPermits()}.
     *
     * @param permits the number of permits to attach
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    public void attach(int permits);

    /**
     * Returns the current number of permits attached to the caller's address. This
     * value has no affect on the semaphores operation unless the caller address
     * actually becomes disconnected.
     * <p/>
     * Positive value is the number of permits that will automatically be
     * {@link #release() released} to the semaphore if the caller address becomes
     * disconnected from the cluster.
     * <p/>
     * Negative value represents the number of permits that the semaphore will
     * automatically be {@link #reducePermits(int) reduced} by if the caller address
     * becomes disconnected from the cluster.
     * <p/>
     * <ul><li>This method is typically used for debugging and testing purposes.
     * </ul>
     *
     * @return the number of permits attached to caller addresses
     */
    public int attachedPermits();

    /**
     * Returns the current number of permits currently available in this semaphore.
     * <p/>
     * <ul><li>This method is typically used for debugging and testing purposes.
     * </ul>
     *
     * @return the number of permits available in this semaphore
     */
    public int availablePermits();

    /**
     * Detaches a permit from the caller's address.
     * <p/>
     * See {@link #attachedPermits()}.
     */
    public void detach();

    /**
     * Detaches the given number of permits from the caller's address.
     * <p/>
     * See {@link #attachedPermits()}.
     *
     * @param permits the number of permits to detach
     */
    public void detach(int permits);

    /**
     * Acquires and returns all permits that are immediately available.
     *
     * @return the number of permits drained
     */
    public int drainPermits();

    /**
     * Shrinks the number of available permits by the indicated
     * reduction. This method differs from {@code acquire} in that it does not
     * block waiting for permits to become available.
     *
     * @param reduction the number of permits to remove
     * @throws IllegalArgumentException if {@code reduction} is negative
     */
    public void reducePermits(int reduction);

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
    public void release();

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
    public void release(int permits);


    /**
     * Detaches a permit from the caller's address and returns it to the semaphore.
     * <p/>
     * See {@link #release()} and {@link #detach()}.
     */
    public void releaseDetach();

    /**
     * Detaches the given number of permits from the caller's address and returns
     * them to the semaphore.
     * <p/>
     * See {@link #release(int)} and {@link #detach()}.
     *
     * @param permits the number of permits to release and detach
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    public void releaseDetach(int permits);

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
    public boolean tryAcquire();

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
    public boolean tryAcquire(int permits);

    /**
     * Acquires a permit from this semaphore, if one becomes available
     * within the given waiting time and the current thread has not
     * been {@linkplain Thread#interrupt interrupted}.
     * <p/>
     * Acquires a permit, if one is available and returns immediately,
     * with the value {@code true},
     * reducing the number of available permits by one.
     * <p/>
     * If no permit is available then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until
     * one of three things happens:
     * <ul>
     * <li>Some other thread invokes the {@link #release} method for this
     * semaphore and the current thread is next to be assigned a permit; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     * <li>The specified waiting time elapses.
     * </ul>
     * <p/>
     * If a permit is acquired then the value {@code true} is returned.
     * <p/>
     * If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     * <p/>
     * <p>If the ISemaphore instance is destroyed while the thread is waiting
     * then {@link InstanceDestroyedException} will be thrown.
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
     * @throws InstanceDestroyedException if the instance is destroyed while waiting
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalStateException    if hazelcast instance is shutdown while waiting
     */
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException;

    /**
     * Acquires the given number of permits, if they are available and
     * returns immediately, with the value {@code true},
     * reducing the number of available permits by the given amount.
     * <p/>
     * If insufficient permits are available then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of three things happens:
     * <ul>
     * <li>Some other thread invokes one of the {@link #release() release}
     * methods for this semaphore, the current thread is next to be assigned
     * permits and the number of available permits satisfies this request; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     * <li>The specified waiting time elapses.
     * </ul>
     * <p/>
     * If the permits are acquired then the value {@code true} is returned.
     * <p/>
     * If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     * <p/>
     * <p>If the ISemaphore instance is destroyed while the thread is waiting
     * then {@link InstanceDestroyedException} will be thrown.
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * for a permit,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @param permits the number of permits to acquire
     * @param timeout the maximum time to wait for the permits
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if all permits were acquired and {@code false}
     *         if the waiting time elapsed before all permits could be acquired
     * @throws InstanceDestroyedException if the instance is destroyed while waiting
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalArgumentException   if {@code permits} is negative
     * @throws IllegalStateException    if hazelcast instance is shutdown while waiting
     */
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException;

    /**
     * Acquires a permit from this semaphore and attaches it to the
     * calling member, only if one is available at the time of invocation.
     * <p/>
     * See {@link #tryAcquire()} and {@link #attach()}.
     *
     * @return {@code true} if permit was acquired and attached to
     *         the caller's address
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    public boolean tryAcquireAttach();

    /**
     * Acquires the given number of permits from this semaphore and
     * attaches them to the calling member, only if all are available
     * at the time of invocation.
     * <p/>
     * See {@link #tryAcquire(int permits)} and {@link #attach(int)}.
     *
     * @param permits the number of permits to try to acquire and attach
     * @return {@code true} if permit(s) were acquired and attached to
     *         the caller's address
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    public boolean tryAcquireAttach(int permits);

    /**
     * Acquires a permit from this semaphore and attaches it to the calling
     * member, only if one becomes available within the given waiting
     * time and the current thread has not been {@linkplain Thread#interrupt interrupted}
     * or the instance destroyed.
     * <p/>
     * If the ISemaphore instance is destroyed while the thread is waiting
     * then {@link InstanceDestroyedException} will be thrown.
     * <p/>
     * See {@link #tryAcquire(long timeout, TimeUnit unit)} and {@link #attach()}.
     *
     * @param timeout the maximum time to wait for a permit
     * @param unit    unit the time unit of the {@code timeout} argument
     * @return {@code true} if a permit was acquired and {@code false}
     *         if the waiting time elapsed before a permit could be acquired
     * @throws InstanceDestroyedException if the instance is destroyed while waiting
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalStateException    if hazelcast instance is shutdown while waiting
     */
    public boolean tryAcquireAttach(long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException;

    /**
     * Acquires the given number of permits from this semaphore and
     * attaches them to the calling member, only if all become available
     * within the given waiting time and the current thread has not
     * been {@linkplain Thread#interrupt interrupted} or the instance
     * destroyed.
     * <p/>
     * See {@link #tryAcquire(int permits, long timeout, TimeUnit unit)} and {@link #attach(int)}.
     *
     * @param permits the number of permits to try to acquire and attach
     * @param timeout the maximum time to wait for the permits
     * @param unit    unit the time unit of the {@code timeout} argument
     * @return {@code true} if permit(s) were acquired and attached to
     *         the caller's address and {@code false} if the waiting
     *         time elapsed before the permits could be acquired
     * @throws InstanceDestroyedException if the instance is destroyed while waiting
     * @throws InterruptedException       if the current thread is interrupted
     * @throws IllegalArgumentException   if {@code permits} is negative
     * @throws IllegalStateException    if hazelcast instance is shutdown while waiting
     */
    public boolean tryAcquireAttach(int permits, long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException;
    
    LocalSemaphoreStats getLocalSemaphoreStats();
}

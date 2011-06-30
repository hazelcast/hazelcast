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

import java.util.concurrent.TimeUnit;

public interface ISemaphore extends Instance {

    /**
     * Returns the name of this collection
     *
     * @return name of this collection
     */
    String getName();

    /**
     * Acquires a permit from this semaphore, blocking until one is
     * available, or the thread is {@linkplain Thread#interrupt interrupted}.
     *
     * @throws InterruptedException
     */
    public void acquire() throws InterruptedException;

    /**
     * Acquires a permit from this semaphore, blocking until one is
     * available.
     */
    public void acquireUninterruptibly();

    /**
     * Acquires a permit from this semaphore, only if one is available at the
     * time of invocation.
     */
    public boolean tryAcquire();

    /**
     * Acquires a permit from this semaphore, if one becomes available
     * within the given waiting time and the current thread has not
     * been {@linkplain Thread#interrupt interrupted}.
     *
     * @param timeout
     * @param unit
     * @return
     */
    public boolean tryAcquire(long timeout, TimeUnit unit);

    /**
     * Releases a permit, returning it to the semaphore.
     */
    public void release();

    /**
     * Acquires the given number of permits from this semaphore,
     * blocking until all are available,
     * or the thread is interrupted.
     *
     * @param permits
     * @throws InterruptedException
     */
    public void acquire(int permits) throws InterruptedException;

    /**
     * Acquires the given number of permits from this semaphore,
     * blocking until all are available.
     *
     * @param permits
     */
    public void acquireUninterruptibly(int permits);

    /**
     * Acquires the given number of permits from this semaphore, only
     * if all are available at the time of invocation.
     *
     * @param permits
     * @return
     */
    public boolean tryAcquire(int permits);

    /**
     * Acquires the given number of permits from this semaphore, if all
     * become available within the given waiting time and the current
     * thread has not been {@linkplain Thread#interrupt interrupted}.
     *
     * @param permits
     * @param timeout
     * @param unit
     * @return
     */
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit);

    /**
     * Releases the given number of permits, returning them to the semaphore.
     *
     * @param permits
     */
    public void release(int permits);

    /**
     * Returns the current number of permits available in this semaphore.
     *
     * @return the number of permits available in this semaphore
     */
    public int availablePermits();

    /**
     * Acquires and returns all permits that are immediately available.
     *
     * @return the number of permits acquired
     */
    public int drainPermits();

    /**
     * Shrinks the number of available permits by the indicated reduction.
     *
     * @param permits
     */
    public void reducePermits(int permits);
}

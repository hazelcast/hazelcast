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

import com.hazelcast.monitor.LocalLockStats;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public interface ILock extends Lock, DistributedObject {

    /**
     * Returns the lock object, the key for this lock instance.
     *
     * @return lock object.
     */
    Object getKey();

    LocalLockStats getLocalLockStats();

    boolean isLocked();

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
     * @param timeUnit unit of time to specify lease time.
     */
    void lock(long leaseTime, TimeUnit timeUnit);

    /**
     * Releases the lock regardless of the lock owner.
     * It always successfully unlocks, never blocks  and returns immediately.
     */
    void forceUnlock();

    ICondition newCondition(String name);
}

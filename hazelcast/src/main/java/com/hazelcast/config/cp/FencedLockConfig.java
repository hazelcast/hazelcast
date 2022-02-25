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

package com.hazelcast.config.cp;

import com.hazelcast.cp.lock.FencedLock;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;

/**
 * Contains configuration options for {@link FencedLock}
 */
public class FencedLockConfig {

    /**
     * The default reentrant lock acquire limit of {@link FencedLock}.
     * 0 means there is no upper bound for number of reentrant lock acquires.
     */
    public static final int DEFAULT_LOCK_ACQUIRE_LIMIT = 0;


    /**
     * Name of FencedLock
     */
    private String name;

    /**
     * Maximum number of reentrant lock acquires. Once a caller acquires
     * the lock this many times, it will not be able to acquire the lock again,
     * until it makes at least one unlock() call.
     * <p>
     * By default, no upper bound is set for the number of reentrant lock
     * acquires, which means that once a caller acquires a {@link FencedLock},
     * all of its further lock() calls will succeed. However, for instance,
     * if you set {@code #lockAcquireLimit} to 2, once a caller acquires
     * the lock, it will be able to acquire it once more, but its third lock()
     * call will not succeed.
     * <p>
     * If {@code #lockAcquireLimit} is set to 1, then the lock becomes
     * non-reentrant.
     */
    private int lockAcquireLimit = DEFAULT_LOCK_ACQUIRE_LIMIT;

    public FencedLockConfig() {
        super();
    }

    public FencedLockConfig(String name) {
        this.name = name;
    }

    public FencedLockConfig(String name, int lockAcquireLimit) {
        this.name = name;
        this.lockAcquireLimit = lockAcquireLimit;
    }

    FencedLockConfig(FencedLockConfig config) {
        this.name = config.name;
        this.lockAcquireLimit = config.lockAcquireLimit;
    }

    /**
     * Returns the name of FencedLock
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of FencedLock
     */
    public FencedLockConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Returns the maximum number of lock acquires a lock holder can perform.
     * 0 means no upper-bound on the number of reentrant lock acquires.
     * 1 means the lock is non-reentrant. It can also be bigger than 1.
     */
    public int getLockAcquireLimit() {
        return lockAcquireLimit;
    }

    /**
     * Sets the number of lock acquires a lock holder can perform.
     */
    public FencedLockConfig setLockAcquireLimit(int lockAcquireLimit) {
        checkNotNegative(lockAcquireLimit, "reentrant lock acquire limit cannot be negative");
        this.lockAcquireLimit = lockAcquireLimit;
        return this;
    }

    /**
     * Disable reentrancy. It means once the lock is acquired, the caller
     * will not be able to acquire it again before it releases the lock.
     */
    public FencedLockConfig disableReentrancy() {
        this.lockAcquireLimit = 1;
        return this;
    }

    /**
     * Enables reentrancy. It means once the lock is acquired, the
     * caller can acquire the lock again many times, without having
     * an upper bound on the number of reentrant acquires.
     */
    public FencedLockConfig enableReentrancy() {
        this.lockAcquireLimit = DEFAULT_LOCK_ACQUIRE_LIMIT;
        return this;
    }

    @Override
    public String toString() {
        return "FencedLockConfig{" + "name='" + name + '\'' + ", lockAcquireLimit=" + lockAcquireLimit + '}';
    }
}

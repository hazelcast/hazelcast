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

package com.hazelcast.internal.util;

import com.hazelcast.cluster.Address;

import java.util.UUID;

/**
 * Object that provides additional functionalities to a simple lock. The user can define a lock owner and a lock expiry
 * and as such use a local lock as a cluster-wide lock if used properly.
 * The {@code lockOwnerId} defines the owner of the lock. One example of the lock owner ID would be a transaction ID.
 * The guard has an expiration time defined during construction.
 */
public class LockGuard {

    public static final LockGuard NOT_LOCKED = new LockGuard();

    /**
     * The ID of the owner that acquired this lock
     */
    private final UUID lockOwnerId;
    /**
     * Owner endpoint, required for logging only
     */
    private final Address lockOwner;
    /**
     * Expiry time for lock lease
     */
    private final long lockExpiryTime;

    private LockGuard() {
        this.lockOwner = null;
        this.lockOwnerId = null;
        this.lockExpiryTime = 0L;
    }

    public LockGuard(Address lockOwner, UUID lockOwnerId, long leaseTime) {
        Preconditions.checkNotNull(lockOwner);
        Preconditions.checkNotNull(lockOwnerId);
        Preconditions.checkPositive("leaseTime", leaseTime);

        this.lockOwner = lockOwner;
        this.lockOwnerId = lockOwnerId;
        this.lockExpiryTime = toLockExpiry(leaseTime);
    }

    private static long toLockExpiry(long leaseTime) {
        long expiryTime = Clock.currentTimeMillis() + leaseTime;
        if (expiryTime < 0L) {
            expiryTime = Long.MAX_VALUE;
        }
        return expiryTime;
    }

    public boolean isLocked() {
        return lockOwner != null;
    }

    public boolean isLeaseExpired() {
        return lockExpiryTime > 0L && Clock.currentTimeMillis() > lockExpiryTime;
    }

    public boolean allowsLock(UUID ownerId) {
        Preconditions.checkNotNull(ownerId);
        boolean notLocked = isLeaseExpired() || !isLocked();
        return notLocked || allowsUnlock(ownerId);
    }

    public boolean allowsUnlock(UUID ownerId) {
        Preconditions.checkNotNull(ownerId);
        return ownerId.equals(lockOwnerId);
    }

    public Address getLockOwner() {
        return lockOwner;
    }

    public UUID getLockOwnerId() {
        return lockOwnerId;
    }

    public long getLockExpiryTime() {
        return lockExpiryTime;
    }

    public long getRemainingTime() {
        return Math.max(0, getLockExpiryTime() - Clock.currentTimeMillis());
    }

    @Override
    public String toString() {
        return "LockGuard{"
                + "lockOwner=" + lockOwner
                + ", lockOwnerId='" + lockOwnerId + '\''
                + ", lockExpiryTime=" + lockExpiryTime
                + '}';
    }
}

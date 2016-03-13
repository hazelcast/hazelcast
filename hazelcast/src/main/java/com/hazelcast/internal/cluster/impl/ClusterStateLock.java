/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.nio.Address;
import com.hazelcast.util.Clock;
import com.hazelcast.util.Preconditions;

/**
 * Lock record to hold transaction id and expiry time
 * in transactions started to change cluster state.
 *
 * @see ClusterState
 * @see com.hazelcast.core.Cluster#changeClusterState(ClusterState, com.hazelcast.transaction.TransactionOptions)
 */
class ClusterStateLock {

    static final ClusterStateLock NOT_LOCKED = new ClusterStateLock();

    /**
     * Transaction that acquired this lock
     */
    private final String transactionId;
    /**
     * Owner endpoint, required for logging only
     */
    private final Address lockOwner;
    /**
     * Expiry time for lock lease
     */
    private final long lockExpiryTime;

    private ClusterStateLock() {
        this.lockOwner = null;
        this.transactionId = null;
        this.lockExpiryTime = 0L;
    }

    ClusterStateLock(Address lockOwner, String transactionId, long leaseTime) {
        Preconditions.checkNotNull(lockOwner);
        Preconditions.checkNotNull(transactionId);
        Preconditions.checkPositive(leaseTime, "Lease time should be positive!");

        this.lockOwner = lockOwner;
        this.transactionId = transactionId;
        this.lockExpiryTime = toLockExpiry(leaseTime);
    }

    private static long toLockExpiry(long leaseTime) {
        long expiryTime = Clock.currentTimeMillis() + leaseTime;
        if (expiryTime < 0L) {
            expiryTime = Long.MAX_VALUE;
        }
        return expiryTime;
    }

    boolean isLocked() {
        return lockOwner != null;
    }

    boolean isLeaseExpired() {
        boolean expired = false;
        if (lockExpiryTime > 0L && Clock.currentTimeMillis() > lockExpiryTime) {
            expired = true;
        }
        return expired;
    }

    boolean allowsLock(String txnId) {
        Preconditions.checkNotNull(txnId);
        boolean notLocked = isLeaseExpired() || !isLocked();
        return notLocked || allowsUnlock(txnId);
    }

    boolean allowsUnlock(String txnId) {
        Preconditions.checkNotNull(txnId);
        return txnId.equals(transactionId);
    }

    Address getLockOwner() {
        return lockOwner;
    }

    String getTransactionId() {
        return transactionId;
    }

    long getLockExpiryTime() {
        return lockExpiryTime;
    }

    @Override
    public String toString() {
        return "ClusterStateLock{"
                + "lockOwner=" + lockOwner
                + ", transactionId='" + transactionId + '\''
                + ", lockExpiryTime=" + lockExpiryTime
                + '}';
    }
}

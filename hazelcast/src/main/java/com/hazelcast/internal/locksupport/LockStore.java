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

package com.hazelcast.internal.locksupport;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.Set;
import java.util.UUID;

/**
 * A container for multiple locks. Each lock is differentiated by a different {@code key}.
 */
public interface LockStore {

    /**
     * Lock a specific key.
     *
     * @param key         the key to lock
     * @param caller      the identifier for the caller
     * @param threadId    the identifier for the thread on the caller
     * @param referenceId the identifier for the invocation of the caller (e.g. operation call ID)
     * @param leaseTime   the lease duration in milliseconds
     * @return if the lock was successfully acquired
     */
    boolean lock(Data key, UUID caller, long threadId, long referenceId, long leaseTime);

    /**
     * Lock a specific key on a local partition only. Does not observe LOCK_MAX_LEASE_TIME_SECONDS
     *
     * @param key         the key to lock
     * @param caller      the identifier for the caller
     * @param threadId    the identifier for the thread on the caller
     * @param referenceId the identifier for the invocation of the caller (e.g. operation call ID)
     * @param leaseTime   the lease duration in milliseconds
     * @return if the lock was successfully acquired
     * @see ClusterProperty#LOCK_MAX_LEASE_TIME_SECONDS
     */
    boolean localLock(Data key, UUID caller, long threadId, long referenceId, long leaseTime);

    /**
     * Lock a specific key for use inside a transaction.
     *
     * @param key         the key to lock
     * @param caller      the identifier for the caller
     * @param threadId    the identifier for the thread on the caller
     * @param referenceId the identifier for the invocation of the caller (e.g. operation call ID)
     * @param leaseTime   the lease duration in milliseconds
     * @param blockReads  whether reads for the key should be blocked (e.g. for map and multimap)
     * @return if the lock was successfully acquired
     */
    boolean txnLock(Data key, UUID caller, long threadId, long referenceId, long leaseTime, boolean blockReads);

    /**
     * Extend the lease time for an already locked key.
     *
     * @param key       the locked key
     * @param caller    the identifier for the caller
     * @param threadId  the identifier for the thread on the caller
     * @param leaseTime time in milliseconds for the lease to be extended
     * @return if the lease was extended
     */
    boolean extendLeaseTime(Data key, UUID caller, long threadId, long leaseTime);

    /**
     * Unlock a specific key.
     *
     * @param key         the locked key
     * @param caller      the identifier for the caller
     * @param threadId    the identifier for the thread on the caller
     * @param referenceId the identifier for the invocation of the caller (e.g. operation call ID)
     * @return if the key was unlocked
     */
    boolean unlock(Data key, UUID caller, long threadId, long referenceId);

    /**
     * Check if a key is locked by any caller and thread ID.
     *
     * @param key the key
     * @return if the key is locked
     */
    boolean isLocked(Data key);

    /**
     * Check if a key is locked by a specific caller and thread ID.
     *
     * @param key      the locked key
     * @param caller   the identifier for the caller
     * @param threadId the identifier for the thread on the caller
     * @return if the key is locked
     */
    boolean isLockedBy(Data key, UUID caller, long threadId);

    /**
     * Return the number of times a key was locked by the same owner (re-entered).
     *
     * @param key the key
     * @return the reentry count
     */
    int getLockCount(Data key);

    /**
     * Return the number of locks this store holds.
     *
     * @return the lock count
     */
    int getLockedEntryCount();

    /**
     * Return the remaining lease time for a specific key.
     *
     * @param key the key
     * @return the lease time in milliseconds, -1 if there is no lock, {@value Long#MAX_VALUE} if the key is locked indefinitely
     */
    long getRemainingLeaseTime(Data key);

    /**
     * Return if the key can be locked by the caller and thread ID.
     *
     * @param key      the locked key
     * @param caller   the identifier for the caller
     * @param threadId the identifier for the thread on the caller
     * @return if the key can be locked. Returns false if the key is already locked by a different caller or thread ID
     */
    boolean canAcquireLock(Data key, UUID caller, long threadId);

    /**
     * Return whether the reads for the specific key should be blocked
     * (see {@link #txnLock(Data, UUID, long, long, long, boolean)}).
     *
     * @param key the lock key
     * @return if the reads should be blocked
     */
    boolean shouldBlockReads(Data key);

    /**
     * Return all locked keys for this store. A lock may be expired when this method returns.
     *
     * @return all locked keys
     */
    Set<Data> getLockedKeys();

    /**
     * Unlock the key regardless of the owner. May return {@code true} if the key is already unlocked but has
     * waiters/signals/expired operations.
     *
     * @param dataKey the lock key
     * @return {@code false} if there is no lock for the given {@code key} and {@code true} otherwise
     */
    boolean forceUnlock(Data dataKey);

    /**
     * Return a string representation of the owner of the lock for a specific key. If the key is not locked returns
     * a default string depicting no owner.
     *
     * @param dataKey the key
     * @return a string description/representation of the owner
     */
    String getOwnerInfo(Data dataKey);
}

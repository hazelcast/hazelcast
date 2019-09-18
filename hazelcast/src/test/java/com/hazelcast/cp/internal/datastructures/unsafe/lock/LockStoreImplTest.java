/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.unsafe.lock;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LockStoreImplTest extends HazelcastTestSupport {

    private static final ObjectNamespace OBJECT_NAME_SPACE = new DistributedObjectNamespace("service", "object");
    private static final int BACKUP_COUNT = 0;
    private static final int ASYNC_BACKUP_COUNT = 0;

    private LockService mockLockServiceImpl;
    private LockStoreImpl lockStore;

    private Data key = new HeapData();
    private String callerId = "called";
    private long threadId = 1;
    private long referenceId = 1;
    private long leaseTime = Long.MAX_VALUE;

    @Before
    public void setUp() {
        mockLockServiceImpl = mock(LockService.class);
        when(mockLockServiceImpl.getMaxLeaseTimeInMillis()).thenReturn(Long.MAX_VALUE);
        EntryTaskScheduler<Data, Integer> mockScheduler = mock(EntryTaskScheduler.class);
        lockStore = new LockStoreImpl(mockLockServiceImpl, OBJECT_NAME_SPACE, mockScheduler, BACKUP_COUNT, ASYNC_BACKUP_COUNT);
    }

    @Test
    public void testLock_whenUnlocked_thenReturnTrue() {
        boolean isLocked = lockAndIncreaseReferenceId();
        assertTrue(isLocked);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLock_whenMaximumLeaseTimeExceeded_thenThrowException() {
        when(mockLockServiceImpl.getMaxLeaseTimeInMillis()).thenReturn(1L);
        lockAndIncreaseReferenceId();
    }

    @Test
    public void testLock_whenLockedBySameThread_thenReturnTrue() {
        lockAndIncreaseReferenceId();
        boolean isLocked = lockAndIncreaseReferenceId();
        assertTrue(isLocked);
    }

    @Test
    public void testLock_whenLockedByDifferentThread_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        threadId++;
        boolean isLocked = lockAndIncreaseReferenceId();
        assertFalse(isLocked);
    }

    @Test
    public void testGetRemainingLeaseTime_whenLockDoesNotExist_thenReturnNegativeOne() {
        long remainingLeaseTime = lockStore.getRemainingLeaseTime(key);
        assertEquals(-1, remainingLeaseTime);
    }

    @Test
    public void testGetRemainingLeaseTime_whenUnlocked_thenReturnNegativeOne() {
        lockAndIncreaseReferenceId();
        unlockAndIncreaseReferenceId();

        long remainingLeaseTime = lockStore.getRemainingLeaseTime(key);
        assertEquals(-1, remainingLeaseTime);
    }

    @Test
    public void testGetRemainingLeaseTime_whenLocked_thenReturnLeaseTime() {
        leaseTime = Long.MAX_VALUE / 2;
        lockAndIncreaseReferenceId();

        long remainingLeaseTime = lockStore.getRemainingLeaseTime(key);
        assertThat(remainingLeaseTime, lessThanOrEqualTo(leaseTime));
        assertThat(remainingLeaseTime, greaterThan(0L));
    }

    @Test
    public void testGetVersion_whenLockDoesNotExist_thenReturnNegativeOne() {
        int version = lockStore.getVersion(key);
        assertEquals(-1, version);
    }

    @Test
    public void testGetVersion_whenUnlocked_thenReturnNegativeOne() {
        lockAndIncreaseReferenceId();
        unlockAndIncreaseReferenceId();
        int version = lockStore.getVersion(key);
        assertEquals(-1, version);
    }

    @Test
    public void testGetVersion_whenLockedOnce_thenReturnPositiveOne() {
        lockAndIncreaseReferenceId();
        int version = lockStore.getVersion(key);
        assertEquals(1, version);
    }

    @Test
    public void testGetVersion_whenLockedTwice_thenReturnPositiveTwo() {
        lockAndIncreaseReferenceId();
        lockAndIncreaseReferenceId();
        int version = lockStore.getVersion(key);
        assertEquals(2, version);
    }

    @Test
    public void testIsLockedBy_whenLockDoesNotExist_thenReturnFalse() {
        boolean lockedBy = lockStore.isLockedBy(key, callerId, threadId);
        assertFalse(lockedBy);
    }

    @Test
    public void testIsLockedBy_whenLockedBySameCallerAndSameThread_thenReturnTrue() {
        lockAndIncreaseReferenceId();
        boolean lockedBy = lockStore.isLockedBy(key, callerId, threadId);
        assertTrue(lockedBy);
    }

    @Test
    public void testIsLockedBy_whenLockedBySameCallerAndDifferentThread_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        long differentThreadId = threadId + 1;
        boolean lockedBy = lockStore.isLockedBy(key, callerId, differentThreadId);
        assertFalse(lockedBy);
    }

    @Test
    public void testIsLockedBy_whenLockedByDifferentCallerAndSameThread_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        String differentCaller = callerId + "different";
        boolean lockedBy = lockStore.isLockedBy(key, differentCaller, threadId);
        assertFalse(lockedBy);
    }

    @Test
    public void testIsLockedBy_whenLockedByDifferentCallerAndDifferentThread_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        long differentThreadId = threadId + 1;
        String differentCaller = callerId + "different";
        boolean lockedBy = lockStore.isLockedBy(key, differentCaller, differentThreadId);
        assertFalse(lockedBy);
    }

    @Test
    public void testIsLocked_whenLockDoesNotExist_thenReturnFalse() {
        boolean locked = lockStore.isLocked(key);
        assertFalse(locked);
    }

    @Test
    public void testIsLocked_whenLockedAndUnlocked_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        unlockAndIncreaseReferenceId();
        boolean locked = lockStore.isLocked(key);
        assertFalse(locked);
    }

    @Test
    public void testIsLocked_whenLocked_thenReturnTrue() {
        lockAndIncreaseReferenceId();
        boolean locked = lockStore.isLocked(key);
        assertTrue(locked);
    }

    @Test
    public void testCanAcquireLock_whenLockDoesNotExist_thenReturnTrue() {
        boolean canAcquire = lockStore.canAcquireLock(key, callerId, threadId);
        assertTrue(canAcquire);
    }

    @Test
    public void testCanAcquireLock_whenLockedBySameThreadAndSameCaller_thenReturnTrue() {
        lockAndIncreaseReferenceId();
        boolean canAcquire = lockStore.canAcquireLock(key, callerId, threadId);
        assertTrue(canAcquire);
    }

    @Test
    public void testCanAquireLock_whenLockedBySameThreadAndDifferentCaller_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        String differentCaller = callerId + "different";
        boolean canAcquire = lockStore.canAcquireLock(key, differentCaller, threadId);
        assertFalse(canAcquire);
    }

    @Test
    public void testCanAcquireLock_whenLockedByDifferentThreadAndSameCaller_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        long differentThreadId = threadId + 1;
        boolean canAcquire = lockStore.canAcquireLock(key, callerId, differentThreadId);
        assertFalse(canAcquire);
    }

    @Test
    public void testCanAcquireLock_whenLockedByDifferentThreadAndDifferentCaller_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        long differentThreadId = threadId + 1;
        String differentCaller = callerId + "different";
        boolean canAcquire = lockStore.canAcquireLock(key, differentCaller, differentThreadId);
        assertFalse(canAcquire);
    }

    @Test
    public void testCanAcquireLock_whenLockedAndUnlocked_thenReturnTrue() {
        lockAndIncreaseReferenceId();
        unlockAndIncreaseReferenceId();
        long differentThreadId = threadId + 1;
        String differentCaller = callerId + "different";
        boolean canAcquire = lockStore.canAcquireLock(key, differentCaller, differentThreadId);
        assertTrue(canAcquire);
    }

    @Test
    public void testForceUnlock_whenLockDoesNotExists_thenReturnFalse() {
        boolean unlocked = lockStore.forceUnlock(key);
        assertFalse(unlocked);
    }

    @Test
    public void testForceUnlock_whenLocked_thenReturnTrue() {
        lockAndIncreaseReferenceId();
        boolean unlocked = lockStore.forceUnlock(key);
        assertTrue(unlocked);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocks_returnUnmodifiableCollection() {
        Collection<LockResource> locks = lockStore.getLocks();
        locks.clear();
    }

    @Test
    public void testGetLocks_whenNoLockExist_thenReturnEmptyCollection() {
        Collection<LockResource> locks = lockStore.getLocks();
        assertThat(locks, is(empty()));
    }

    @Test
    public void testGetLocks_whenLocked_thenReturnCollectionWithSingleItem() {
        lockAndIncreaseReferenceId();
        Collection<LockResource> locks = lockStore.getLocks();
        assertThat(locks, hasSize(1));
    }

    @Test
    public void testGetLockedEntryCount() {
        lock();
        assertEquals(1, lockStore.getLockedEntryCount());
    }

    @Test
    public void testGetLockCount_whenLockDoesNotExist_thenReturnZero() {
        int lockCount = lockStore.getLockCount(key);
        assertThat(lockCount, is(0));
    }

    @Test
    public void testGetLockCount_whenLockedOnce_thenReturnOne() {
        lockAndIncreaseReferenceId();
        int lockCount = lockStore.getLockCount(key);
        assertThat(lockCount, is(1));
    }

    @Test
    public void testGetLockCount_whenLockedTwice_thenReturnTwo() {
        lockAndIncreaseReferenceId();
        lockAndIncreaseReferenceId();
        int lockCount = lockStore.getLockCount(key);
        assertThat(lockCount, is(2));
    }

    @Test
    public void testGetLockCount_whenLockedTwiceWithTheSameReferenceId_thenReturnOne() {
        lock();
        lockAndIncreaseReferenceId();
        int lockCount = lockStore.getLockCount(key);
        assertThat(lockCount, is(1));
    }

    @Test
    public void testUnlock_whenLockDoesNotExist_thenReturnFalse() {
        boolean unlocked = unlockAndIncreaseReferenceId();
        assertFalse(unlocked);
    }

    @Test
    public void testUnlock_whenLockedBySameCallerAndSameThreadId_thenReturnTrue() {
        lockAndIncreaseReferenceId();
        boolean unlocked = unlockAndIncreaseReferenceId();
        assertTrue(unlocked);
    }

    @Test
    public void testUnlock_whenLockedByDifferentCallerAndSameThreadId_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        callerId += "different";
        boolean unlocked = unlockAndIncreaseReferenceId();
        assertFalse(unlocked);
    }

    @Test
    public void testIsLocked_whenTxnLockedAndUnlockedWithSameReferenceId_thenReturnFalse() {
        //see https://github.com/hazelcast/hazelcast/issues/5923 for details
        txnLock();
        unlock();
        boolean locked = lockStore.isLocked(key);
        assertFalse(locked);
    }

    @Test
    public void testUnlock_whenLockedBySameCallerAndDifferentThreadId_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        threadId++;
        boolean unlocked = unlockAndIncreaseReferenceId();
        assertFalse(unlocked);
    }

    @Test
    public void testUnlock_whenLockedByDifferentCallerAndDifferentThreadId_thenReturnFalse() {
        lockAndIncreaseReferenceId();
        threadId++;
        callerId += "different";
        boolean unlocked = unlockAndIncreaseReferenceId();
        assertFalse(unlocked);
    }

    @Test
    public void testUnlock_whenLockedTwiceWithSameReferenceIdAndUnlockedOnce_thenReturnTrue() {
        lock();
        lockAndIncreaseReferenceId();
        boolean unlocked = unlockAndIncreaseReferenceId();
        assertTrue(unlocked);
    }

    @Test
    public void testTxnLock_whenLockDoesNotExist_thenResultTrue() {
        boolean locked = txnLockAndIncreaseReferenceId();
        assertTrue(locked);
    }

    @Test
    public void testTxnLock_whenLockedByDifferentCallerAndSameThreadId_thenReturnFalse() {
        txnLockAndIncreaseReferenceId();
        callerId += "different";
        boolean locked = txnLockAndIncreaseReferenceId();
        assertFalse(locked);
    }

    @Test
    public void testTxnLock_whenLockedBySameCallerAndDifferentThreadId_thenReturnFalse() {
        txnLockAndIncreaseReferenceId();
        threadId++;
        boolean locked = txnLockAndIncreaseReferenceId();
        assertFalse(locked);
    }

    @Test
    public void testIsBlockReads_whenLockDoesNotExist_thenReturnFalse() {
        boolean locked = lockStore.shouldBlockReads(key);
        assertFalse(locked);
    }

    @Test
    public void testIsIsBlockReads_whenNonTxnLocked_thenReturnFalse() {
        lock();
        boolean locked = lockStore.shouldBlockReads(key);
        assertFalse(locked);
    }

    @Test
    public void testIsBlockReads_whenTxnLocked_thenReturnTrue() {
        txnLock();
        boolean locked = lockStore.shouldBlockReads(key);
        assertTrue(locked);
    }

    @Test
    public void testIsBlockReads_whenTxnLockedAndUnlocked_thenReturnFalse() {
        txnLockAndIncreaseReferenceId();
        unlock();
        boolean locked = lockStore.shouldBlockReads(key);
        assertFalse(locked);
    }

    @Test
    public void testIsBlockReads_whenTxnLockedAndAttemptedToLockFromAnotherThread_thenReturnTrue() {
        txnLockAndIncreaseReferenceId();
        threadId++;
        lockAndIncreaseReferenceId();
        boolean locked = lockStore.shouldBlockReads(key);
        assertTrue(locked);
    }

    private boolean lock() {
        return lockStore.lock(key, callerId, threadId, referenceId, leaseTime);
    }

    private boolean txnLock() {
        return lockStore.txnLock(key, callerId, threadId, referenceId, leaseTime, true);
    }

    private boolean unlock() {
        return lockStore.unlock(key, callerId, threadId, referenceId);
    }

    private boolean lockAndIncreaseReferenceId() {
        boolean isLocked = lock();
        referenceId++;
        return isLocked;
    }

    private boolean txnLockAndIncreaseReferenceId() {
        boolean isLocked = txnLock();
        referenceId++;
        return isLocked;
    }

    private boolean unlockAndIncreaseReferenceId() {
        boolean isUnlocked = unlock();
        referenceId++;
        return isUnlocked;
    }
}

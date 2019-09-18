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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static com.hazelcast.cp.internal.datastructures.unsafe.lock.LockStoreProxy.NOT_LOCKED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests LockStoreProxy when the internal LockStoreImpl has been cleared.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LockStoreProxyTest extends HazelcastTestSupport {

    private static final int PARTITION_ID = 1;
    private static final ObjectNamespace NAMESPACE = new DistributedObjectNamespace(MapService.SERVICE_NAME, "test");

    private Data key = new HeapData();
    private String callerId = "called";
    private long threadId = 1;
    private long otherThreadId = 2;
    private long referenceId = 1;
    private long leaseTimeInfinite = Long.MAX_VALUE;
    private long leaseTimeShort = 60000L;

    private HazelcastInstance instance;
    private LockStoreProxy lockStoreProxy;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
        LockService lockService = getNodeEngineImpl(instance).getService(LockService.SERVICE_NAME);
        // create a LockStoreProxy
        lockStoreProxy = (LockStoreProxy) lockService.createLockStore(PARTITION_ID, NAMESPACE);
        // clear the lock store -> the LockStoreImpl no longer exists
        lockService.clearLockStore(PARTITION_ID, NAMESPACE);
    }

    @Test
    public void lock() {
        assertTrue(lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeInfinite));
        assertFalse(lockStoreProxy.canAcquireLock(key, callerId, otherThreadId));
    }

    @Test
    public void localLock() {
        assertTrue(lockStoreProxy.localLock(key, callerId, threadId, referenceId, leaseTimeInfinite));
        assertFalse(lockStoreProxy.canAcquireLock(key, callerId, otherThreadId));
    }

    @Test
    public void txnLock() {
        assertTrue(lockStoreProxy.txnLock(key, callerId, threadId, referenceId, leaseTimeInfinite, true));
        assertFalse(lockStoreProxy.canAcquireLock(key, callerId, otherThreadId));
    }

    @Test
    public void extendLeaseTime_whenLockStoreImplIsNull() {
        assertFalse(lockStoreProxy.extendLeaseTime(key, callerId, threadId, leaseTimeInfinite));
    }

    @Test
    public void extendLeaseTime_whenLockWasLocked() {
        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeShort);
        assertTrue(lockStoreProxy.extendLeaseTime(key, callerId, threadId, leaseTimeInfinite));
    }

    @Test
    public void unlock_whenLockStoreImplIsNull() {
        assertFalse(lockStoreProxy.unlock(key, callerId, threadId, referenceId));
    }

    @Test
    public void unlock_whenLockWasLocked() {
        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeShort);
        assertTrue(lockStoreProxy.unlock(key, callerId, threadId, referenceId));
    }

    @Test
    public void isLocked_whenLockStoreImplIsNull() {
        assertFalse(lockStoreProxy.isLocked(key));
    }

    @Test
    public void isLocked_whenLockWasLocked() {
        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeShort);
        assertTrue(lockStoreProxy.isLocked(key));
    }

    @Test
    public void isLockedBy_whenLockStoreImplIsNull() {
        assertFalse(lockStoreProxy.isLockedBy(key, callerId, threadId));
    }

    @Test
    public void isLockedBy_whenLockWasLocked() {
        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeShort);
        assertTrue(lockStoreProxy.isLockedBy(key, callerId, threadId));
    }

    @Test
    public void getLockCount() {
        assertEquals(0, lockStoreProxy.getLockCount(key));

        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeShort);
        assertEquals(1, lockStoreProxy.getLockCount(key));
    }

    @Test
    public void getLockedEntryCount() {
        assertEquals(0, lockStoreProxy.getLockedEntryCount());

        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeShort);
        assertEquals(1, lockStoreProxy.getLockedEntryCount());
    }

    @Test
    public void getRemainingLeaseTime_whenLockStoreImplIsNull() {
        assertEquals(0, lockStoreProxy.getRemainingLeaseTime(key));
    }

    @Test
    public void getRemainingLeaseTime_whenLockWasLocked() {
        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeShort);
        assertTrue(lockStoreProxy.getRemainingLeaseTime(key) > 0);
    }

    @Test
    public void canAcquireLock_whenLockWasLocked() {
        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeInfinite);
        assertFalse(lockStoreProxy.canAcquireLock(key, callerId, otherThreadId));
    }

    @Test
    public void canAcquireLock_whenLockStoreImplIsNull() {
        assertTrue(lockStoreProxy.canAcquireLock(key, callerId, threadId));
    }

    @Test
    public void shouldBlockReads_whenLockWasLocked() {
        lockStoreProxy.txnLock(key, callerId, threadId, referenceId, leaseTimeInfinite, true);
        assertTrue(lockStoreProxy.shouldBlockReads(key));
    }

    @Test
    public void shouldBlockReads_whenLockStoreImplIsNull() {
        assertFalse(lockStoreProxy.shouldBlockReads(key));
    }

    @Test
    public void getLockedKeys_whenLockWasLocked() {
        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeInfinite);
        assertEquals(Collections.singleton(key), lockStoreProxy.getLockedKeys());
    }

    @Test
    public void getLockedKeys_whenLockStoreImplIsNull() {
        assertEquals(Collections.emptySet(), lockStoreProxy.getLockedKeys());
    }

    @Test
    public void forceUnlock_whenLockWasLocked() {
        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeInfinite);
        assertTrue(lockStoreProxy.forceUnlock(key));
    }

    @Test
    public void forceUnlock_whenLockStoreImplIsNull() {
        assertFalse(lockStoreProxy.forceUnlock(key));
    }

    @Test
    public void getOwnerInfo_whenLockStoreImplIsNull() {
        assertEquals(NOT_LOCKED, lockStoreProxy.getOwnerInfo(key));
    }

    @Test
    public void getOwnerInfo_whenLockWasLocked() {
        lockStoreProxy.lock(key, callerId, threadId, referenceId, leaseTimeInfinite);
        assertContains(lockStoreProxy.getOwnerInfo(key), callerId);
    }
}

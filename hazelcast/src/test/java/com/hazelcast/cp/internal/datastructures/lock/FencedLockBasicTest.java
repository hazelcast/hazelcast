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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.lock.proxy.AbstractRaftFencedLockProxy;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.lock.exception.LockOwnershipLostException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.cp.lock.FencedLock.INVALID_FENCE;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockBasicTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected HazelcastInstance lockInstance;
    protected FencedLock lock;
    private String objectName = "lock";
    private String proxyName = objectName + "@group1";

    @Before
    public void setup() {
        instances = createInstances();
        lock = lockInstance.getCPSubsystem().getLock(proxyName);
        assertNotNull(lock);
    }

    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = newInstances(3);
        lockInstance = instances[RandomPicker.getInt(instances.length)];
        return instances;
    }

    protected AbstractProxySessionManager getSessionManager(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    @Test
    public void testLock_whenNotLocked() {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
        assertEquals(fence, lock.getFence());
    }

    @Test
    public void testLockInterruptibly_whenNotLocked() throws InterruptedException {
        lock.lockInterruptibly();
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
        assertValidFence(lock.getFence());
    }

    @Test
    public void testLock_whenLockedBySelf() {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        long newFence = lock.lockAndGetFence();
        assertEquals(fence, newFence);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test
    public void testLock_whenLockedByOther() throws InterruptedException {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
        assertTrue(lock.isLockedByCurrentThread());

        CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            lock.lock();
            latch.countDown();
        });

        t.start();
        assertFalse(latch.await(5000, TimeUnit.MILLISECONDS));
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlock_whenFree() {
        lock.unlock();
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testGetFence_whenFree() {
        lock.getFence();
    }

    @Test
    public void testIsLocked_whenFree() {
        assertFalse(lock.isLocked());
    }

    @Test
    public void testIsLockedByCurrentThread_whenFree() {
        assertFalse(lock.isLockedByCurrentThread());
    }

    @Test
    public void testGetLockCount_whenFree() {
        assertEquals(0, lock.getLockCount());
    }

    @Test
    public void testUnlock_whenLockedBySelf() {
        lock.lock();

        lock.unlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
        try {
            lock.getFence();
            fail();
        } catch (IllegalMonitorStateException ignored) {
        }
    }

    @Test
    public void testUnlock_whenReentrantlyLockedBySelf() {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);
        lock.lock();
        lock.unlock();

        assertTrue(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
        assertEquals(fence, lock.getFence());
    }

    @Test(timeout = 60000)
    public void testLock_Unlock_thenLock() {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);
        lock.unlock();

        AtomicReference<Long> newFenceRef = new AtomicReference<>();
        spawn(() -> {
            long newFence = lock.lockAndGetFence();
            newFenceRef.set(newFence);
        });

        assertTrueEventually(() -> assertNotNull(newFenceRef.get()));

        assertTrue(newFenceRef.get() > fence);
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
        assertFalse(lock.isLockedByCurrentThread());
        try {
            lock.getFence();
            fail();
        } catch (IllegalMonitorStateException ignored) {
        }
    }

    @Test(timeout = 60000)
    public void testUnlock_whenPendingLockOfOtherThread() {
        long fence = lock.lockAndGetFence();
        AtomicReference<Long> newFenceRef = new AtomicReference<>();
        spawn(() -> {
            long newFence = lock.tryLockAndGetFence(60, TimeUnit.SECONDS);
            newFenceRef.set(newFence);
        });

        assertTrueEventually(() -> {
            RaftLockService service = getNodeEngineImpl(instances[0]).getService(RaftLockService.SERVICE_NAME);
            RaftLockRegistry registry = service.getRegistryOrNull(lock.getGroupId());
            assertNotNull(registry);
            assertFalse(registry.getWaitTimeouts().isEmpty());
        });

        lock.unlock();

        assertTrueEventually(() -> assertNotNull(newFenceRef.get()));

        assertTrue(newFenceRef.get() > fence);

        assertTrue(lock.isLocked());
        assertFalse(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
        try {
            lock.getFence();
            fail();
        } catch (IllegalMonitorStateException ignored) {
        }
    }

    @Test(timeout = 60000)
    public void testUnlock_whenLockedByOther() {
        lockByOtherThread(lock);

        try {
            lock.unlock();
            fail();
        } catch (IllegalMonitorStateException ignored) {
        }

        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testTryLock_whenNotLocked() {
        long fence = lock.tryLockAndGetFence();

        assertValidFence(fence);
        assertEquals(fence, lock.getFence());
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testTryLock_whenLockedBySelf() {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        long newFence = lock.tryLockAndGetFence();
        assertEquals(fence, newFence);
        assertEquals(fence, lock.getFence());
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenLockedByOther() {
        lockByOtherThread(lock);

        long fence = lock.tryLockAndGetFence();

        assertInvalidFence(fence);
        assertTrue(lock.isLocked());
        assertFalse(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testTryLockTimeout() {
        long fence = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);

        assertValidFence(fence);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testTryLockTimeout_whenLockedBySelf() {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        long newFence = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);

        assertEquals(fence, newFence);
        assertEquals(fence, lock.getFence());
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockTimeout_whenLockedByOther() {
        lockByOtherThread(lock);

        long fence = lock.tryLockAndGetFence(100, TimeUnit.MILLISECONDS);

        assertInvalidFence(fence);
        assertTrue(lock.isLocked());
        assertFalse(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockLongTimeout_whenLockedByOther() {
        lockByOtherThread(lock);

        long fence = lock.tryLockAndGetFence(RaftLockService.WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS + 1, TimeUnit.MILLISECONDS);

        assertInvalidFence(fence);
        assertTrue(lock.isLocked());
        assertFalse(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testReentrantLockFails_whenSessionClosed() throws ExecutionException, InterruptedException {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        AbstractProxySessionManager sessionManager = getSessionManager(lockInstance);
        RaftGroupId groupId = (RaftGroupId) lock.getGroupId();
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        closeSession(instances[0], groupId, sessionId);

        assertTrueEventually(() -> assertNotEquals(sessionId, sessionManager.getSession(groupId)));

        try {
            lock.lock();
        } catch (LockOwnershipLostException ignored) {
        }
    }

    @Test
    public void testReentrantTryLockFails_whenSessionClosed() throws ExecutionException, InterruptedException {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        AbstractProxySessionManager sessionManager = getSessionManager(lockInstance);
        RaftGroupId groupId = (RaftGroupId) lock.getGroupId();
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        closeSession(instances[0], groupId, sessionId);

        assertTrueEventually(() -> assertNotEquals(sessionId, sessionManager.getSession(groupId)));

        try {
            lock.tryLock();
        } catch (LockOwnershipLostException ignored) {
        }
    }

    @Test
    public void testReentrantTryLockWithTimeoutFails_whenSessionClosed() throws ExecutionException, InterruptedException {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        AbstractProxySessionManager sessionManager = getSessionManager(lockInstance);
        RaftGroupId groupId = (RaftGroupId) lock.getGroupId();
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        closeSession(instances[0], groupId, sessionId);

        assertTrueEventually(() -> assertNotEquals(sessionId, sessionManager.getSession(groupId)));

        try {
            lock.tryLock(1, TimeUnit.SECONDS);
        } catch (LockOwnershipLostException ignored) {
        }
    }

    @Test
    public void testUnlockFails_whenSessionClosed() throws ExecutionException, InterruptedException {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        AbstractProxySessionManager sessionManager = getSessionManager(lockInstance);
        RaftGroupId groupId = (RaftGroupId) lock.getGroupId();
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        closeSession(instances[0], groupId, sessionId);

        assertTrueEventually(() -> assertNotEquals(sessionId, sessionManager.getSession(groupId)));

        try {
            lock.unlock();
        } catch (LockOwnershipLostException ignored) {
        }

        assertFalse(lock.isLockedByCurrentThread());
        assertFalse(lock.isLocked());
        assertNoLockedSessionId();
    }

    @Test
    public void testUnlockFails_whenNewSessionCreated() throws ExecutionException, InterruptedException {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        AbstractProxySessionManager sessionManager = getSessionManager(lockInstance);
        RaftGroupId groupId = (RaftGroupId) lock.getGroupId();
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        closeSession(instances[0], groupId, sessionId);

        assertTrueEventually(() -> assertNotEquals(sessionId, sessionManager.getSession(groupId)));

        lockByOtherThread(lock);

        // now we have a new session

        try {
            lock.unlock();
        } catch (LockOwnershipLostException ignored) {
        }

        assertFalse(lock.isLockedByCurrentThread());
    }

    @Test
    public void testGetFenceFails_whenNewSessionCreated() throws ExecutionException, InterruptedException {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        AbstractProxySessionManager sessionManager = getSessionManager(lockInstance);
        RaftGroupId groupId = (RaftGroupId) lock.getGroupId();
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        closeSession(instances[0], groupId, sessionId);

        assertTrueEventually(() -> assertNotEquals(sessionId, sessionManager.getSession(groupId)));

        lockByOtherThread(lock);

        // now we have a new session

        try {
            lock.getFence();
        } catch (LockOwnershipLostException ignored) {
        }

        assertFalse(lock.isLockedByCurrentThread());
    }

    @Test(timeout = 60000)
    public void testFailedTryLock_doesNotAcquireSession() {
        lockByOtherThread(lock);

        AbstractProxySessionManager sessionManager = getSessionManager(lockInstance);
        RaftGroupId groupId = (RaftGroupId) lock.getGroupId();
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);
        assertEquals(1, sessionManager.getSessionAcquireCount(groupId, sessionId));

        long fence = lock.tryLockAndGetFence();
        assertInvalidFence(fence);
        assertEquals(1, sessionManager.getSessionAcquireCount(groupId, sessionId));
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void test_destroy() {
        lock.lock();
        lock.destroy();

        assertNoLockedSessionId();
        lock.lock();
    }

    @Test
    public void test_lockInterruptibly() throws InterruptedException {
        HazelcastInstance newInstance = factory.newHazelcastInstance(createConfig(3, 3));
        FencedLock lock = newInstance.getCPSubsystem().getLock("lock@group1");
        lock.lockInterruptibly();
        lock.unlock();

        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> ref = new AtomicReference<>();

        Thread thread = new Thread(() -> {
            try {
                latch.countDown();
                lock.lockInterruptibly();
            } catch (Throwable t) {
                ref.set(t);
            }
        });
        thread.start();

        assertOpenEventually(latch);

        thread.interrupt();

        assertTrueEventually(() -> {
            Throwable t = ref.get();
            assertTrue(t instanceof InterruptedException);
        });
    }

    @Test
    public void test_lockFailsAfterCPGroupDestroyed() throws ExecutionException, InterruptedException {
        instances[0].getCPSubsystem()
                    .getCPSubsystemManagementService()
                    .forceDestroyCPGroup(lock.getGroupId().name())
                    .get();

        try {
            lock.lock();
            fail();
        } catch (CPGroupDestroyedException ignored) {
        }

        lockInstance.getCPSubsystem().getLock(proxyName);
    }

    static void assertValidFence(long fence) {
        assertNotEquals(INVALID_FENCE, fence);
    }

    static void assertInvalidFence(long fence) {
        assertEquals(INVALID_FENCE, fence);
    }

    private void assertNoLockedSessionId() {
        if (lock instanceof AbstractRaftFencedLockProxy) {
            assertNull(((AbstractRaftFencedLockProxy) lock).getLockedSessionId(getThreadId()));
        }
    }

    private void closeSession(HazelcastInstance instance, CPGroupId groupId, long sessionId) throws ExecutionException, InterruptedException {
        instance.getCPSubsystem().getCPSessionManagementService().forceCloseSession(groupId.name(), sessionId).get();
    }

    static void lockByOtherThread(FencedLock lock) {
        try {
            spawn(lock::lock).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

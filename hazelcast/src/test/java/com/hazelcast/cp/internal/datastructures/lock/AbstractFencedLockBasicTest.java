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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.lock.proxy.AbstractFencedLockProxy;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.session.operation.CloseSessionOp;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.lock.exception.LockOwnershipLostException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.cp.lock.FencedLock.INVALID_FENCE;
import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractFencedLockBasicTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected HazelcastInstance primaryInstance;
    protected HazelcastInstance proxyInstance;
    protected FencedLock lock;

    @Before
    public void setup() {
        instances = createInstances();
        primaryInstance = getPrimaryInstance();
        proxyInstance = getProxyInstance();
        lock = proxyInstance.getCPSubsystem().getLock(getProxyName());
        assertNotNull(lock);
    }

    protected abstract String getProxyName();

    protected abstract HazelcastInstance[] createInstances();

    protected abstract HazelcastInstance getPrimaryInstance();

    protected HazelcastInstance getProxyInstance() {
        return getPrimaryInstance();
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
            LockService service = getNodeEngineImpl(instances[0]).getService(LockService.SERVICE_NAME);
            LockRegistry registry = service.getRegistryOrNull(lock.getGroupId());
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

        long fence = lock.tryLockAndGetFence(LockService.WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS + 1, TimeUnit.MILLISECONDS);

        assertInvalidFence(fence);
        assertTrue(lock.isLocked());
        assertFalse(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testReentrantLockFails_whenSessionClosed() throws ExecutionException, InterruptedException {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        AbstractProxySessionManager sessionManager = getSessionManager(proxyInstance);
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

        AbstractProxySessionManager sessionManager = getSessionManager(proxyInstance);
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

        AbstractProxySessionManager sessionManager = getSessionManager(proxyInstance);
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

        AbstractProxySessionManager sessionManager = getSessionManager(proxyInstance);
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

        AbstractProxySessionManager sessionManager = getSessionManager(proxyInstance);
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

        AbstractProxySessionManager sessionManager = getSessionManager(proxyInstance);
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

    @Test
    public void testFailedTryLock_doesNotAcquireSession() {
        lockByOtherThread(lock);

        AbstractProxySessionManager sessionManager = getSessionManager(proxyInstance);
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

    protected AbstractProxySessionManager getSessionManager(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    private void assertNoLockedSessionId() {
        if (lock instanceof AbstractFencedLockProxy) {
            assertNull(((AbstractFencedLockProxy) lock).getLockedSessionId(getThreadId()));
        }
    }

    private void closeSession(HazelcastInstance instance, CPGroupId groupId, long sessionId) {
        RaftService service = getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
        service.getInvocationManager().invoke(groupId, new CloseSessionOp(sessionId)).joinInternal();
    }

    static void assertValidFence(long fence) {
        assertNotEquals(INVALID_FENCE, fence);
    }

    static void assertInvalidFence(long fence) {
        assertEquals(INVALID_FENCE, fence);
    }

    static void lockByOtherThread(FencedLock lock) {
        try {
            spawn(lock::lock).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

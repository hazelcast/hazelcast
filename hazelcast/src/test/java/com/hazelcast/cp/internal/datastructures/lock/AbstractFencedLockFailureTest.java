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
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.lock.operation.LockOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.TryLockOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.UnlockOp;
import com.hazelcast.cp.internal.datastructures.lock.proxy.RaftFencedLockProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKeyContainer;
import com.hazelcast.cp.internal.datastructures.spi.blocking.operation.ExpireWaitKeysOp;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cp.internal.datastructures.lock.FencedLockBasicTest.assertInvalidFence;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.cp.lock.FencedLock.INVALID_FENCE;
import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractFencedLockFailureTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected HazelcastInstance primaryInstance;
    protected HazelcastInstance proxyInstance;
    protected RaftFencedLockProxy lock;
    protected String objectName = "lock";

    @Before
    public void setup() {
        instances = createInstances();
        primaryInstance = getPrimaryInstance();
        proxyInstance = getProxyInstance();
        lock = (RaftFencedLockProxy) proxyInstance.getCPSubsystem().getLock(getProxyName());
        assertNotNull(lock);
    }

    protected abstract String getProxyName();

    protected abstract HazelcastInstance[] createInstances();

    protected abstract HazelcastInstance getPrimaryInstance();

    protected HazelcastInstance getProxyInstance() {
        return getPrimaryInstance();
    }

    @Test
    public void testRetriedLockDoesNotCancelPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            RaftLockService service = getNodeEngineImpl(primaryInstance).getService(RaftLockService.SERVICE_NAME);
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid));

        assertTrueAllTheTime(() -> {
            RaftLockService service = getNodeEngineImpl(primaryInstance).getService(RaftLockService.SERVICE_NAME);
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            assertEquals(1, registry.getWaitTimeouts().size());
        }, 10);
    }

    @Test
    public void testNewLockCancelsPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            RaftLockService service = getNodeEngineImpl(primaryInstance).getService(RaftLockService.SERVICE_NAME);
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid2));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test
    public void testRetriedTryLockWithTimeoutDoesNotCancelPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        NodeEngineImpl nodeEngine = getNodeEngineImpl(primaryInstance);
        RaftLockService service = nodeEngine.getService(RaftLockService.SERVICE_NAME);

        assertTrueEventually(() -> {
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertNotNull(registry.getResourceOrNull(objectName));
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            RaftService raftService = getNodeEngineImpl(primaryInstance).getService(RaftService.SERVICE_NAME);
            int partitionId = raftService.getCPGroupPartitionId(groupId);
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            boolean[] verified = new boolean[1];
            CountDownLatch latch = new CountDownLatch(1);
            OperationServiceImpl operationService = nodeEngine.getOperationService();
            operationService.execute(new PartitionSpecificRunnable() {
                @Override
                public int getPartitionId() {
                    return partitionId;
                }

                @Override
                public void run() {
                    RaftLock raftLock = registry.getResourceOrNull(objectName);
                    Map<Object, WaitKeyContainer<LockInvocationKey>> waitKeys = raftLock.getInternalWaitKeysMap();
                    verified[0] = (waitKeys.size() == 1 && waitKeys.values().iterator().next().retryCount() == 1);
                    latch.countDown();
                }
            });

            latch.await(60, SECONDS);

            assertTrue(verified[0]);
        });
    }

    @Test
    public void testNewTryLockWithTimeoutCancelsPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            RaftLockService service = getNodeEngineImpl(primaryInstance).getService(RaftLockService.SERVICE_NAME);
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid2, MINUTES.toMillis(5)));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test
    public void testRetriedTryLockWithoutTimeoutDoesNotCancelPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            RaftLockService service = getNodeEngineImpl(primaryInstance).getService(RaftLockService.SERVICE_NAME);
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, 0));

        assertTrueAllTheTime(() -> {
            RaftLockService service = getNodeEngineImpl(primaryInstance).getService(RaftLockService.SERVICE_NAME);
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            assertEquals(1, registry.getWaitTimeouts().size());
        }, 10);
    }

    @Test
    public void testNewUnlockCancelsPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid = newUnsecureUUID();

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            RaftLockService service = getNodeEngineImpl(primaryInstance).getService(RaftLockService.SERVICE_NAME);
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        try {
            lock.unlock();
            fail();
        } catch (IllegalMonitorStateException ignored) {
        }

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test
    public void testLockAcquireRetry() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid)).join();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid)).join();

        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testLockReentrantAcquireRetry() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid1)).join();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid2)).join();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid2)).join();

        assertEquals(2, lock.getLockCount());
    }

    @Test
    public void testPendingLockAcquireRetry() {
        CountDownLatch unlockLatch = new CountDownLatch(1);
        spawn(() -> {
            lock.lock();
            assertOpenEventually(unlockLatch);
            lock.unlock();
        });

        assertTrueEventually(() -> assertTrue(lock.isLocked()));

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid = newUnsecureUUID();

        InternalCompletableFuture<Long> f1 = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            RaftLockService service = getNodeEngineImpl(primaryInstance).getService(RaftLockService.SERVICE_NAME);
            assertFalse(service.getRegistryOrNull(groupId).getWaitTimeouts().isEmpty());
        });

        unlockLatch.countDown();

        assertTrueEventually(() -> {
            RaftLockService service = getNodeEngineImpl(primaryInstance).getService(RaftLockService.SERVICE_NAME);
            assertTrue(service.getRegistryOrNull(groupId).getWaitTimeouts().isEmpty());
            assertTrue(lock.isLocked());
        });

        InternalCompletableFuture<Long> f2 = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        long fence1 = f1.join();
        long fence2 = f2.join();

        assertEquals(fence1, lock.getFence());
        assertEquals(fence1, fence2);
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testRetriedUnlockIsSuccessfulAfterLockedByAnotherEndpoint() {
        lock.lock();

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new UnlockOp(objectName, sessionId, getThreadId(), invUid)).join();

        lockByOtherThread();

        invocationManager.invoke(groupId, new UnlockOp(objectName, sessionId, getThreadId(), invUid)).join();
    }

    @Test
    public void testIsLockedByCurrentThreadCallInitializesLockedSessionId() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        long threadId = getThreadId();
        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, threadId, newUnsecureUUID())).join();

        // the current thread acquired the lock once and we pretend that there was a operation timeout in lock.lock() call

        assertTrue(lock.isLockedByCurrentThread());
        Long lockedSessionId = lock.getLockedSessionId(threadId);
        assertNotNull(lockedSessionId);
        assertEquals(sessionId, (long) lockedSessionId);
    }

    @Test
    public void testLockCallInitializesLockedSessionId() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        long threadId = getThreadId();
        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, threadId, newUnsecureUUID())).join();

        lock.lock();

        Long lockedSessionId = lock.getLockedSessionId(threadId);
        assertNotNull(lockedSessionId);
        assertEquals(sessionId, (long) lockedSessionId);
    }

    @Test
    public void testUnlockCallInitializesLockedSessionId() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        long threadId = getThreadId();
        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, threadId, newUnsecureUUID())).join();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, threadId, newUnsecureUUID())).join();

        lock.unlock();

        Long lockedSessionId = lock.getLockedSessionId(threadId);
        assertNotNull(lockedSessionId);
        assertEquals(sessionId, (long) lockedSessionId);
    }

    @Test
    public void testIsLockedCallInitializesLockedSessionId() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        long threadId = getThreadId();
        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, threadId, newUnsecureUUID())).join();

        boolean locked = lock.isLocked();

        assertTrue(locked);
        Long lockedSessionId = lock.getLockedSessionId(threadId);
        assertNotNull(lockedSessionId);
        assertEquals(sessionId, (long) lockedSessionId);
    }

    @Test
    public void testGetLockCountCallInitializesLockedSessionId() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        long threadId = getThreadId();
        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, threadId, newUnsecureUUID())).join();

        int getLockCount = lock.getLockCount();

        assertEquals(1, getLockCount);
        Long lockedSessionId = lock.getLockedSessionId(threadId);
        assertNotNull(lockedSessionId);
        assertEquals(sessionId, (long) lockedSessionId);
    }

    @Test
    public void testRetriedWaitKeysAreExpiredTogether() {
        CountDownLatch releaseLatch = new CountDownLatch(1);
        spawn(() -> {
            lock.lock();
            assertOpenEventually(releaseLatch);
            lock.unlock();
        });

        assertTrueEventually(() -> assertTrue(lock.isLocked()));

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager();
        UUID invUid = newUnsecureUUID();
        BiTuple[] lockWaitTimeoutKeyRef = new BiTuple[1];

        InternalCompletableFuture<Long> f1 = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, SECONDS.toMillis(300)));

        NodeEngineImpl nodeEngine = getNodeEngineImpl(primaryInstance);
        RaftLockService service = nodeEngine.getService(RaftLockService.SERVICE_NAME);

        assertTrueEventually(() -> {
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            Map<BiTuple<String, UUID>, BiTuple<Long, Long>> waitTimeouts = registry.getWaitTimeouts();
            assertEquals(1, waitTimeouts.size());
            lockWaitTimeoutKeyRef[0] = waitTimeouts.keySet().iterator().next();
        });

        InternalCompletableFuture<Long> f2 = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, SECONDS.toMillis(300)));

        assertTrueEventually(() -> {
            RaftService raftService = getNodeEngineImpl(primaryInstance).getService(RaftService.SERVICE_NAME);
            int partitionId = raftService.getCPGroupPartitionId(groupId);
            RaftLockRegistry registry = service.getRegistryOrNull(groupId);
            boolean[] verified = new boolean[1];
            CountDownLatch latch = new CountDownLatch(1);
            OperationServiceImpl operationService = nodeEngine.getOperationService();
            operationService.execute(new PartitionSpecificRunnable() {
                @Override
                public int getPartitionId() {
                    return partitionId;
                }

                @Override
                public void run() {
                    RaftLock raftLock = registry.getResourceOrNull(objectName);
                    Map<Object, WaitKeyContainer<LockInvocationKey>> waitKeys = raftLock.getInternalWaitKeysMap();
                    verified[0] = (waitKeys.size() == 1 && waitKeys.values().iterator().next().retryCount() == 1);
                    latch.countDown();
                }
            });

            assertOpenEventually(latch);
            assertTrue(verified[0]);
        });

        RaftOp op = new ExpireWaitKeysOp(RaftLockService.SERVICE_NAME, Collections.singletonList(lockWaitTimeoutKeyRef[0]));
        invocationManager.invoke(groupId, op).join();

        assertTrueEventually(() -> assertTrue(service.getRegistryOrNull(groupId).getWaitTimeouts().isEmpty()));

        releaseLatch.countDown();

        assertTrueEventually(() -> assertFalse(lock.isLocked()));

        long fence1 = f1.join();
        long fence2 = f2.join();

        assertInvalidFence(fence1);
        assertInvalidFence(fence2);
    }

    @Test
    public void testExpiredAndRetriedTryLockRequestReceivesFailureResponse() {
        final CountDownLatch unlockLatch = new CountDownLatch(1);
        spawn(() -> {
            lock.lock();
            assertOpenEventually(unlockLatch);
            lock.unlock();
        });

        assertTrueEventually(() -> assertTrue(lock.isLocked()));

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);
        UUID invUid = newUnsecureUUID();

        InternalCompletableFuture<Long> f1 = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, SECONDS.toMillis(5)));

        long fence1 = f1.join();
        assertEquals(INVALID_FENCE, fence1);

        unlockLatch.countDown();

        assertTrueEventually(() -> assertFalse(lock.isLocked()));

        InternalCompletableFuture<Long> f2 = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, SECONDS.toMillis(5)));

        long fence2 = f2.join();
        assertEquals(INVALID_FENCE, fence2);
    }

    private RaftInvocationManager getRaftInvocationManager() {
        RaftService service = getNodeEngineImpl(proxyInstance).getService(RaftService.SERVICE_NAME);
        return service.getInvocationManager();
    }

    private void lockByOtherThread() {
        Thread t = new Thread(() -> {
            try {
                lock.lock();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private AbstractProxySessionManager getSessionManager() {
        return getNodeEngineImpl(proxyInstance).getService(ProxySessionManagerService.SERVICE_NAME);
    }
}

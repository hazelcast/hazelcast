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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ChangePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.DrainPermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKeyContainer;
import com.hazelcast.cp.internal.datastructures.spi.blocking.operation.ExpireWaitKeysOp;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.session.RaftSessionService;
import com.hazelcast.cp.internal.session.SessionAwareProxy;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractSemaphoreAdvancedTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected HazelcastInstance primaryInstance;
    protected HazelcastInstance proxyInstance;
    protected ISemaphore semaphore;
    protected String objectName = "semaphore";

    @Before
    public void setup() {
        instances = createInstances();
        primaryInstance = getPrimaryInstance();
        proxyInstance = getProxyInstance();
        semaphore = proxyInstance.getCPSubsystem().getSemaphore(getProxyName());
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getProxyName();

    protected abstract HazelcastInstance getPrimaryInstance();

    protected HazelcastInstance getProxyInstance() {
        return getPrimaryInstance();
    }

    @Test
    public void testSuccessfulAcquireClearsWaitTimeouts() {
        semaphore.init(1);

        CPGroupId groupId = getGroupId();
        HazelcastInstance leader = leaderInstanceOf(groupId);
        SemaphoreService service = getNodeEngineImpl(leader).getService(SemaphoreService.SERVICE_NAME);
        SemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        CountDownLatch latch = new CountDownLatch(1);
        spawn(() -> {
            try {
                semaphore.acquire(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });

        assertTrueEventually(() -> assertFalse(registry.getLiveOperations().isEmpty()));

        semaphore.increasePermits(1);

        assertOpenEventually(latch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testSuccessfulTryAcquireClearsWaitTimeouts() {
        semaphore.init(1);

        CPGroupId groupId = getGroupId();
        HazelcastInstance leader = leaderInstanceOf(groupId);
        SemaphoreService service = getNodeEngineImpl(leader).getService(SemaphoreService.SERVICE_NAME);
        SemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        CountDownLatch latch = new CountDownLatch(1);
        spawn(() -> {
            try {
                semaphore.tryAcquire(2, 10, MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });

        assertTrueEventually(() -> {
            assertFalse(registry.getWaitTimeouts().isEmpty());
            assertFalse(registry.getLiveOperations().isEmpty());
        });

        semaphore.increasePermits(1);

        assertOpenEventually(latch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testFailedTryAcquireClearsWaitTimeouts() throws InterruptedException {
        semaphore.init(1);

        CPGroupId groupId = getGroupId();
        HazelcastInstance leader = leaderInstanceOf(groupId);
        SemaphoreService service = getNodeEngineImpl(leader).getService(SemaphoreService.SERVICE_NAME);
        SemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        boolean success = semaphore.tryAcquire(2, 1, TimeUnit.SECONDS);

        assertFalse(success);
        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testPermitIncreaseClearsWaitTimeouts() {
        semaphore.init(1);

        CPGroupId groupId = getGroupId();
        HazelcastInstance leader = leaderInstanceOf(groupId);
        SemaphoreService service = getNodeEngineImpl(leader).getService(SemaphoreService.SERVICE_NAME);
        SemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        CountDownLatch latch = new CountDownLatch(1);
        spawn(() -> {
            try {
                semaphore.tryAcquire(2, 10, MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            latch.countDown();
        });

        assertTrueEventually(() -> {
            assertFalse(registry.getWaitTimeouts().isEmpty());
            assertFalse(registry.getLiveOperations().isEmpty());
        });

        semaphore.increasePermits(1);

        assertOpenEventually(latch);
        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testDestroyClearsWaitTimeouts() {
        semaphore.init(1);

        CPGroupId groupId = getGroupId();
        HazelcastInstance leader = leaderInstanceOf(groupId);
        SemaphoreService service = getNodeEngineImpl(leader).getService(SemaphoreService.SERVICE_NAME);
        SemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        spawn(() -> {
            try {
                semaphore.tryAcquire(2, 10, MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueEventually(() -> {
            assertFalse(registry.getWaitTimeouts().isEmpty());
            assertFalse(registry.getLiveOperations().isEmpty());
        });

        semaphore.destroy();

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testInactiveSessionsAreEventuallyClosed() throws ExecutionException, InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(groupId).get().isEmpty());
            }
        });

        AbstractProxySessionManager sessionManager = getSessionManager();
        long sessionId = sessionManager.getSession(groupId);

        assertNotEquals(NO_SESSION_ID, sessionId);

        // Not using semaphore.release(), because we want to keep sending session HBs.
        RaftOp op = new ReleasePermitsOp(objectName, sessionId, getThreadId(), newUnsecureUUID(), 1);
        invokeRaftOp(groupId, op).get();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService service1 = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertTrue(service1.getAllSessions(groupId).get().isEmpty());
            }

            assertEquals(NO_SESSION_ID, sessionManager.getSession(groupId));
        });
    }

    @Test
    public void testActiveSessionIsNotClosed() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(getGroupId()).get().isEmpty());
            }
        });

        assertTrueAllTheTime(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(getGroupId()).get().isEmpty());
            }
        }, 20);
    }

    @Test
    public void testActiveSessionWithPendingPermitIsNotClosed() {
        spawn(() -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(getGroupId()).get().isEmpty());
            }
        });

        assertTrueAllTheTime(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(getGroupId()).get().isEmpty());
            }
        }, 20);
    }

    @Test
    public void testRetriedReleaseIsSuccessfulAfterAcquiredByAnotherEndpoint() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        UUID invUid = newUnsecureUUID();

        invokeRaftOp(groupId, new ReleasePermitsOp(objectName, sessionId, getThreadId(), invUid, 1)).joinInternal();

        spawn(() -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        invokeRaftOp(groupId, new ReleasePermitsOp(objectName, sessionId, getThreadId(), invUid, 1)).joinInternal();
    }

    @Test
    public void testRetriedIncreasePermitsAppliedOnlyOnce() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);
        UUID invUid = newUnsecureUUID();

        invokeRaftOp(groupId, new ChangePermitsOp(objectName, sessionId, getThreadId(), invUid, 1)).joinInternal();
        invokeRaftOp(groupId, new ChangePermitsOp(objectName, sessionId, getThreadId(), invUid, 1)).joinInternal();

        assertEquals(2, semaphore.availablePermits());
    }

    @Test
    public void testRetriedDecreasePermitsAppliedOnlyOnce() throws InterruptedException {
        semaphore.init(2);
        semaphore.acquire();
        semaphore.release();
        // we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);
        UUID invUid = newUnsecureUUID();

        invokeRaftOp(groupId, new ChangePermitsOp(objectName, sessionId, getThreadId(), invUid, -1)).joinInternal();
        invokeRaftOp(groupId, new ChangePermitsOp(objectName, sessionId, getThreadId(), invUid, -1)).joinInternal();

        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testRetriedDrainPermitsAppliedOnlyOnce() throws ExecutionException, InterruptedException {
        semaphore.increasePermits(3);

        RaftGroupId groupId = getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);
        UUID invUid = newUnsecureUUID();

        int drained1 = this.<Integer>invokeRaftOp(groupId, new DrainPermitsOp(objectName, sessionId, getThreadId(), invUid)).joinInternal();

        assertEquals(3, drained1);
        assertEquals(0, semaphore.availablePermits());

        spawn(() -> semaphore.increasePermits(1)).get();

        int drained2 = this.<Integer>invokeRaftOp(groupId, new DrainPermitsOp(objectName, sessionId, getThreadId(), invUid)).joinInternal();

        assertEquals(3, drained2);
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testRetriedWaitKeysAreExpiredTogether() {
        semaphore.init(1);

        CountDownLatch releaseLatch = new CountDownLatch(1);
        spawn(() -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            assertOpenEventually(releaseLatch);
            semaphore.release();
        });

        assertTrueEventually(() -> assertEquals(0, semaphore.availablePermits()));

        // there is a session id now

        RaftGroupId groupId = getGroupId();

        AbstractProxySessionManager sessionManager = getSessionManager();
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        UUID invUid = newUnsecureUUID();
        BiTuple[] acquireWaitTimeoutKeyRef = new BiTuple[1];

        InternalCompletableFuture<Boolean> f1 =
                invokeRaftOp(groupId, new AcquirePermitsOp(objectName, sessionId, getThreadId(), invUid, 1, SECONDS.toMillis(300)));

        assertTrueEventually(() -> {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(primaryInstance);
            SemaphoreService service = nodeEngine.getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            Map<BiTuple<String, UUID>, BiTuple<Long, Long>> waitTimeouts = registry.getWaitTimeouts();
            assertEquals(1, waitTimeouts.size());
            acquireWaitTimeoutKeyRef[0] = waitTimeouts.keySet().iterator().next();
        });

        InternalCompletableFuture<Boolean> f2 =
                invokeRaftOp(groupId, new AcquirePermitsOp(objectName, sessionId, getThreadId(), invUid, 1, SECONDS.toMillis(300)));

        assertTrueEventually(() -> {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(primaryInstance);
            RaftService raftService = getNodeEngineImpl(primaryInstance).getService(RaftService.SERVICE_NAME);
            int partitionId = raftService.getCPGroupPartitionId(groupId);
            SemaphoreService service = nodeEngine.getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
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
                    Semaphore semaphore = registry.getResourceOrNull(objectName);
                    Map<Object, WaitKeyContainer<AcquireInvocationKey>> waitKeys = semaphore.getInternalWaitKeysMap();
                    verified[0] = (waitKeys.size() == 1 && waitKeys.values().iterator().next().retryCount() == 1);
                    latch.countDown();
                }
            });

            assertOpenEventually(latch);

            assertTrue(verified[0]);
        });

        RaftOp op = new ExpireWaitKeysOp(SemaphoreService.SERVICE_NAME,
                Collections.singletonList(acquireWaitTimeoutKeyRef[0]));
        invokeRaftOp(groupId, op).joinInternal();

        assertTrueEventually(() -> {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(primaryInstance);
            SemaphoreService service = nodeEngine.getService(SemaphoreService.SERVICE_NAME);
            assertTrue(service.getRegistryOrNull(groupId).getWaitTimeouts().isEmpty());
        });

        releaseLatch.countDown();

        assertTrueEventually(() -> assertEquals(1, semaphore.availablePermits()));

        assertFalse(f1.joinInternal());
        assertFalse(f2.joinInternal());
    }

    @Test
    public void testPermitAcquired_whenPermitOwnerShutsDown() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        CountDownLatch acquiredLatch = new CountDownLatch(1);
        spawn(() -> {
            HazelcastInstance otherInstance = instances[0] == proxyInstance ? instances[1] : instances[0];
            ISemaphore remoteSemaphore = otherInstance.getCPSubsystem().getSemaphore(semaphore.getName());
            try {
                remoteSemaphore.acquire();
                acquiredLatch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(proxyInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(getGroupId());
            assertNotNull(registry);
            Semaphore semaphore = registry.getResourceOrNull(objectName);
            assertNotNull(semaphore);
            assertFalse(semaphore.getInternalWaitKeysMap().isEmpty());
        });

        proxyInstance.shutdown();

        assertOpenEventually(acquiredLatch);
    }

    private AbstractProxySessionManager getSessionManager() {
        return getNodeEngineImpl(proxyInstance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    protected RaftGroupId getGroupId() {
        return ((SessionAwareProxy) semaphore).getGroupId();
    }

    protected abstract <T> InternalCompletableFuture<T> invokeRaftOp(RaftGroupId groupId, RaftOp op);

    protected abstract HazelcastInstance leaderInstanceOf(CPGroupId groupId);
}

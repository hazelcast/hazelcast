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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ChangePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.DrainPermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKeyContainer;
import com.hazelcast.cp.internal.datastructures.spi.blocking.operation.ExpireWaitKeysOp;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.session.RaftSessionService;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftSemaphoreAdvancedTest extends HazelcastRaftTestSupport {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private HazelcastInstance[] instances;
    private HazelcastInstance semaphoreInstance;
    private RaftSessionAwareSemaphoreProxy semaphore;
    private String objectName = "semaphore";
    private String proxyName = objectName + "@group1";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = newInstances(groupSize);
        semaphoreInstance = instances[RandomPicker.getInt(instances.length)];
        semaphore = (RaftSessionAwareSemaphoreProxy) semaphoreInstance.getCPSubsystem().getSemaphore(proxyName);
    }

    @Test
    public void testSuccessfulAcquireClearsWaitTimeouts() {
        semaphore.init(1);

        CPGroupId groupId = semaphore.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
        RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        CountDownLatch latch = new CountDownLatch(1);
        spawn(() -> {
            semaphore.acquire(2);
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

        CPGroupId groupId = semaphore.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
        RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        CountDownLatch latch = new CountDownLatch(1);
        spawn(() -> {
            semaphore.tryAcquire(2, 10, MINUTES);
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
    public void testFailedTryAcquireClearsWaitTimeouts() {
        semaphore.init(1);

        CPGroupId groupId = semaphore.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
        RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        boolean success = semaphore.tryAcquire(2, 1, TimeUnit.SECONDS);

        assertFalse(success);
        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testPermitIncreaseClearsWaitTimeouts() {
        semaphore.init(1);

        CPGroupId groupId = semaphore.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
        RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        CountDownLatch latch = new CountDownLatch(1);
        spawn(() -> {
            semaphore.tryAcquire(2, 10, MINUTES);
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

        CPGroupId groupId = semaphore.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
        RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        spawn(() -> {
            semaphore.tryAcquire(2, 10, MINUTES);
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
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        semaphore.init(1);

        spawn(() -> {
            semaphore.tryAcquire(2, 10, MINUTES);
        });

        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            semaphore.acquire();
            semaphore.release();
        }

        CPGroupId groupId = semaphore.getGroupId();

        assertTrueEventually(() -> {
            HazelcastInstance leader = getLeaderInstance(instances, groupId);
            RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
            RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertFalse(registry.getWaitTimeouts().isEmpty());
        });

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                assertNotNull(raftNode);
                LogEntry snapshotEntry = getSnapshotEntry(raftNode);
                assertTrue(snapshotEntry.index() > 0);
                List<RestoreSnapshotOp> ops = (List<RestoreSnapshotOp>) snapshotEntry.operation();
                for (RestoreSnapshotOp op : ops) {
                    if (op.getServiceName().equals(RaftSemaphoreService.SERVICE_NAME)) {
                        ResourceRegistry registry = (ResourceRegistry) op.getSnapshot();
                        assertFalse(registry.getWaitTimeouts().isEmpty());
                        return;
                    }
                }
                fail();
            }
        });

        instances[1].shutdown();

        HazelcastInstance newInstance = factory.newHazelcastInstance(createConfig(groupSize, groupSize));
        newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember().get();

        assertTrueEventually(() -> {
            RaftSemaphoreService service = getNodeEngineImpl(newInstance).getService(RaftSemaphoreService.SERVICE_NAME);
            RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertFalse(registry.getWaitTimeouts().isEmpty());
            assertEquals(1, registry.availablePermits(objectName));
        });
    }

    @Test
    public void testInactiveSessionsAreEventuallyClosed() throws ExecutionException, InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = semaphore.getGroupId();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(groupId.name()).get().isEmpty());
            }
        });

        NodeEngineImpl nodeEngine = getNodeEngineImpl(semaphoreInstance);
        ProxySessionManagerService service = nodeEngine.getService(ProxySessionManagerService.SERVICE_NAME);
        long sessionId = service.getSession(groupId);

        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftOp op = new ReleasePermitsOp(objectName, sessionId, getThreadId(), newUnsecureUUID(), 1);
        getRaftInvocationManager(semaphoreInstance).invoke(groupId, op).get();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService service1 = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertTrue(service1.getAllSessions(groupId.name()).get().isEmpty());
            }

            assertEquals(NO_SESSION_ID, service.getSession(groupId));
        });
    }

    @Test
    public void testActiveSessionIsNotClosed() {
        semaphore.init(1);
        semaphore.acquire();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(semaphore.getGroupId().name()).get().isEmpty());
            }
        });

        assertTrueAllTheTime(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(semaphore.getGroupId().name()).get().isEmpty());
            }
        }, 20);
    }

    @Test
    public void testActiveSessionWithPendingPermitIsNotClosed() {
        spawn(() -> semaphore.acquire());

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(semaphore.getGroupId().name()).get().isEmpty());
            }
        });

        assertTrueAllTheTime(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(semaphore.getGroupId().name()).get().isEmpty());
            }
        }, 20);
    }

    @Test
    public void testRetriedReleaseIsSuccessfulAfterAcquiredByAnotherEndpoint() {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = semaphore.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        invocationManager.invoke(groupId, new ReleasePermitsOp(objectName, sessionId, getThreadId(), invUid, 1)).join();

        spawn(() -> semaphore.acquire());

        invocationManager.invoke(groupId, new ReleasePermitsOp(objectName, sessionId, getThreadId(), invUid, 1)).join();
    }

    @Test
    public void testRetriedIncreasePermitsAppliedOnlyOnce() {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // we guarantee that there is a session id now...

        RaftGroupId groupId = semaphore.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        invocationManager.invoke(groupId, new ChangePermitsOp(objectName, sessionId, getThreadId(), invUid, 1)).join();
        invocationManager.invoke(groupId, new ChangePermitsOp(objectName, sessionId, getThreadId(), invUid, 1)).join();

        assertEquals(2, semaphore.availablePermits());
    }

    @Test
    public void testRetriedDecreasePermitsAppliedOnlyOnce() {
        semaphore.init(2);
        semaphore.acquire();
        semaphore.release();
        // we guarantee that there is a session id now...

        RaftGroupId groupId = semaphore.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        invocationManager.invoke(groupId, new ChangePermitsOp(objectName, sessionId, getThreadId(), invUid, -1)).join();
        invocationManager.invoke(groupId, new ChangePermitsOp(objectName, sessionId, getThreadId(), invUid, -1)).join();

        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testRetriedDrainPermitsAppliedOnlyOnce() throws ExecutionException, InterruptedException {
        semaphore.increasePermits(3);

        RaftGroupId groupId = semaphore.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        int drained1 = invocationManager.<Integer>invoke(groupId, new DrainPermitsOp(objectName, sessionId, getThreadId(), invUid)).join();

        assertEquals(3, drained1);
        assertEquals(0, semaphore.availablePermits());

        spawn(() -> semaphore.increasePermits(1)).get();

        int drained2 = invocationManager.<Integer>invoke(groupId, new DrainPermitsOp(objectName, sessionId, getThreadId(), invUid)).join();

        assertEquals(3, drained2);
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testRetriedWaitKeysAreExpiredTogether() {
        semaphore.init(1);

        CountDownLatch releaseLatch = new CountDownLatch(1);
        spawn(() -> {
            semaphore.acquire();
            assertOpenEventually(releaseLatch);
            semaphore.release();
        });

        assertTrueEventually(() -> assertEquals(0, semaphore.availablePermits()));

        // there is a session id now

        RaftGroupId groupId = semaphore.getGroupId();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(semaphoreInstance);
        RaftSemaphoreService service = nodeEngine.getService(RaftSemaphoreService.SERVICE_NAME);

        ProxySessionManagerService sessionManager = nodeEngine.getService(ProxySessionManagerService.SERVICE_NAME);
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);
        UUID invUid = newUnsecureUUID();
        Tuple2[] acquireWaitTimeoutKeyRef = new Tuple2[1];

        InternalCompletableFuture<Boolean> f1 = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, getThreadId(), invUid, 1, SECONDS.toMillis(300)));

        assertTrueEventually(() -> {
            RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            Map<Tuple2<String, UUID>, Tuple2<Long, Long>> waitTimeouts = registry.getWaitTimeouts();
            assertEquals(1, waitTimeouts.size());
            acquireWaitTimeoutKeyRef[0] = waitTimeouts.keySet().iterator().next();
        });

        InternalCompletableFuture<Boolean> f2 = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, getThreadId(), invUid, 1, SECONDS.toMillis(300)));

        assertTrueEventually(() -> {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(groupId);
            RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            boolean[] verified = new boolean[1];
            CountDownLatch latch = new CountDownLatch(1);
            OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
            operationService.execute(new PartitionSpecificRunnable() {
                @Override
                public int getPartitionId() {
                    return partitionId;
                }

                @Override
                public void run() {
                    RaftSemaphore raftSemaphore = registry.getResourceOrNull(objectName);
                    Map<Object, WaitKeyContainer<AcquireInvocationKey>> waitKeys = raftSemaphore.getInternalWaitKeysMap();
                    verified[0] = (waitKeys.size() == 1 && waitKeys.values().iterator().next().retryCount() == 1);
                    latch.countDown();
                }
            });

            assertOpenEventually(latch);

            assertTrue(verified[0]);
        });

        RaftOp op = new ExpireWaitKeysOp(RaftSemaphoreService.SERVICE_NAME,
                Collections.singletonList(acquireWaitTimeoutKeyRef[0]));
        invocationManager.invoke(groupId, op).join();

        assertTrueEventually(() -> assertTrue(service.getRegistryOrNull(groupId).getWaitTimeouts().isEmpty()));

        releaseLatch.countDown();

        assertTrueEventually(() -> assertEquals(1, semaphore.availablePermits()));

        assertFalse(f1.join());
        assertFalse(f2.join());
    }

    @Test
    public void testPermitAcquired_whenPermitOwnerShutsDown() {
        semaphore.init(1);
        semaphore.acquire();

        CountDownLatch acquiredLatch = new CountDownLatch(1);
        spawn(() -> {
            HazelcastInstance otherInstance = instances[0] == semaphoreInstance ? instances[1] : instances[0];
            ISemaphore remoteSemaphore = otherInstance.getCPSubsystem().getSemaphore(proxyName);
            try {
                remoteSemaphore.acquire();
                acquiredLatch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueEventually(() -> {
            RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
            RaftSemaphoreRegistry registry = service.getRegistryOrNull(semaphore.getGroupId());
            assertNotNull(registry);
            RaftSemaphore semaphore = registry.getResourceOrNull(objectName);
            assertNotNull(semaphore);
            assertFalse(semaphore.getInternalWaitKeysMap().isEmpty());
        });

        semaphoreInstance.shutdown();

        assertOpenEventually(acquiredLatch);
    }

    private AbstractProxySessionManager getSessionManager() {
        return getNodeEngineImpl(semaphoreInstance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        cpSubsystemConfig.setSessionTimeToLiveSeconds(10);
        cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(1);

        CPSemaphoreConfig semaphoreConfig = new CPSemaphoreConfig(objectName, false);
        cpSubsystemConfig.addSemaphoreConfig(semaphoreConfig);
        return config;
    }
}

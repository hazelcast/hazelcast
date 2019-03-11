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

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.lock.operation.UnlockOp;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.session.RaftSessionService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.datastructures.lock.FencedLockBasicTest.lockByOtherThread;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FencedLockAdvancedTest extends HazelcastRaftTestSupport {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private HazelcastInstance[] instances;
    private HazelcastInstance lockInstance;
    private FencedLock lock;
    private String objectName = "lock";
    private String proxyName = objectName + "@group1";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = createInstances();
        lock = createLock();
        assertNotNull(lock);
    }

    private FencedLock createLock() {
        lockInstance = instances[RandomPicker.getInt(instances.length)];
        return lockInstance.getCPSubsystem().getLock(proxyName);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        cpSubsystemConfig.setSessionTimeToLiveSeconds(10);
        cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(1);

        return config;
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    @Test
    public void testSuccessfulLockClearsWaitTimeouts() {
        lock.lock();

        CPGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        final RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        final RaftLockRegistry registry = service.getRegistryOrNull(groupId);

        final CountDownLatch latch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                latch.countDown();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getLiveOperations().isEmpty());
            }
        });

        lock.unlock();

        assertOpenEventually(latch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }


    @Test
    public void testSuccessfulTryLockClearsWaitTimeouts() {
        lock.lock();

        CPGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        final RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        final RaftLockRegistry registry = service.getRegistryOrNull(groupId);

        final CountDownLatch latch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                lock.tryLock(10, MINUTES);
                latch.countDown();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
                assertFalse(registry.getLiveOperations().isEmpty());
            }
        });

        lock.unlock();

        assertOpenEventually(latch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testFailedTryLockClearsWaitTimeouts() {
        lockByOtherThread(lock);

        CPGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        RaftLockRegistry registry = service.getRegistryOrNull(groupId);

        long fence = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);

        assertEquals(FencedLock.INVALID_FENCE, fence);
        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testDestroyClearsWaitTimeouts() {
        lockByOtherThread(lock);

        CPGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        final RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        final RaftLockRegistry registry = service.getRegistryOrNull(groupId);

        spawn(new Runnable() {
            @Override
            public void run() {
                lock.tryLock(10, MINUTES);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
                assertFalse(registry.getLiveOperations().isEmpty());
            }
        });

        lock.destroy();

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        final long fence = this.lock.lockAndGetFence();
        assertTrue(fence > 0);

        spawn(new Runnable() {
            @Override
            public void run() {
                lock.tryLock(10, MINUTES);
            }
        });

        final CPGroupId groupId = this.lock.getGroupId();

        spawn(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
                    lock.isLocked();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                HazelcastInstance leader = getLeaderInstance(instances, groupId);
                RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
                ResourceRegistry registry = service.getRegistryOrNull(groupId);
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    assertNotNull(raftNode);
                    LogEntry snapshotEntry = getSnapshotEntry(raftNode);
                    assertTrue(snapshotEntry.index() > 0);
                    List<RestoreSnapshotOp> ops = (List<RestoreSnapshotOp>) snapshotEntry.operation();
                    for (RestoreSnapshotOp op : ops) {
                        if (op.getServiceName().equals(RaftLockService.SERVICE_NAME)) {
                            ResourceRegistry registry = (ResourceRegistry) op.getSnapshot();
                            assertFalse(registry.getWaitTimeouts().isEmpty());
                            return;
                        }
                    }
                    fail();
                }
            }
        });

        HazelcastInstance instanceToShutdown = (instances[0] == lockInstance) ? instances[1] : instances[0];
        instanceToShutdown.shutdown();

        final HazelcastInstance newInstance = factory.newHazelcastInstance(createConfig(groupSize, groupSize));
        newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftNodeImpl raftNode = getRaftNode(newInstance, groupId);
                assertNotNull(raftNode);
                assertTrue(getSnapshotEntry(raftNode).index() > 0);

                RaftLockService service = getNodeEngineImpl(newInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getWaitTimeouts().isEmpty());
                RaftLockOwnershipState ownership = registry.getLockOwnershipState(objectName);
                assertTrue(ownership.isLocked());
                assertTrue(ownership.getLockCount() > 0);
                assertEquals(fence, ownership.getFence());
            }
        });
    }

    @Test
    public void testInactiveSessionsAreEventuallyClosed() throws ExecutionException, InterruptedException {
        lock.lock();

        final RaftGroupId groupId = (RaftGroupId) lock.getGroupId();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getAllSessions(groupId.name()).get().isEmpty());
                }
            }
        });

        RaftSessionService sessionService = getNodeEngineImpl(lockInstance).getService(RaftSessionService.SERVICE_NAME);
        long sessionId = sessionService.getAllSessions(groupId.name()).get().iterator().next().id();

        getRaftInvocationManager(lockInstance)
                .invoke(groupId, new UnlockOp(objectName, sessionId, getThreadId(), newUnsecureUUID())).join();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertTrue(service.getAllSessions(groupId.name()).get().isEmpty());
                }

                ProxySessionManagerService service = getNodeEngineImpl(lockInstance).getService(ProxySessionManagerService.SERVICE_NAME);
                assertEquals(NO_SESSION_ID, service.getSession(groupId));
            }
        });
    }

    @Test
    public void testActiveSessionIsNotClosedWhenLockIsHeld() {
        lock.lock();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getAllSessions(lock.getGroupId().name()).get().isEmpty());
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getAllSessions(lock.getGroupId().name()).get().isEmpty());
                }
            }
        }, 20);
    }

    @Test
    public void testActiveSessionIsNotClosedWhenPendingWaitKey() {
        FencedLock other = null;
        for (HazelcastInstance instance : instances) {
            if (instance != lockInstance) {
                other = instance.getCPSubsystem().getLock(proxyName);
                break;
            }
        }

        assertNotNull(other);

        // lock from another instance
        other.lock();

        spawn(new Runnable() {
            @Override
            public void run() {
                lock.tryLock(30, TimeUnit.MINUTES);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertEquals(2, sessionService.getAllSessions(lock.getGroupId().name()).get().size());
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertEquals(2, sessionService.getAllSessions(lock.getGroupId().name()).get().size());
                }
            }
        }, 20);
    }

    @Test
    public void testLockAcquired_whenLockOwnerShutsDown() {
        lock.lock();

        final CountDownLatch remoteLockedLatch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                HazelcastInstance otherInstance = instances[0] == lockInstance ? instances[1] : instances[0];
                FencedLock remoteLock = otherInstance.getCPSubsystem().getLock(proxyName);
                remoteLock.lock();
                remoteLockedLatch.countDown();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(lock.getGroupId());
                assertNotNull(registry);
                RaftLock raftLock = registry.getResourceOrNull(objectName);
                assertNotNull(raftLock);
                assertFalse(raftLock.getInternalWaitKeysMap().isEmpty());
            }
        });

        lockInstance.shutdown();

        assertOpenEventually(remoteLockedLatch);
    }

}

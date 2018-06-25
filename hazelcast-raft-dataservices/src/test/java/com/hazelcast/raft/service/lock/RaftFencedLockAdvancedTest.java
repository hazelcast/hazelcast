package com.hazelcast.raft.service.lock;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.operation.snapshot.RestoreSnapshotOp;
import com.hazelcast.raft.impl.session.SessionService;
import com.hazelcast.raft.service.blocking.ResourceRegistry;
import com.hazelcast.raft.service.lock.proxy.RaftFencedLockProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.service.lock.RaftFencedLockBasicTest.lockByOtherThread;
import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftFencedLockAdvancedTest extends HazelcastRaftTestSupport {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private HazelcastInstance[] instances;
    private HazelcastInstance lockInstance;
    private FencedLock lock;
    private String name = "lock";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = createInstances();
        lock = createLock();
        assertNotNull(lock);
    }

    private FencedLock createLock() {
        lockInstance = instances[RandomPicker.getInt(instances.length)];
        return createLock(lockInstance);
    }

    private FencedLock createLock(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);

        try {
            RaftGroupId groupId = raftService.createRaftGroupForProxy(name);
            String objectName = raftService.getObjectNameForProxy(name);
            SessionManagerService sessionManager = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
            return new RaftFencedLockProxy(raftService.getInvocationManager(), sessionManager, groupId, objectName);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        RaftConfig raftConfig = config.getRaftConfig();
        raftConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        raftConfig.setSessionTimeToLiveSeconds(10);
        raftConfig.setSessionHeartbeatIntervalSeconds(1);

        return config;
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    @Test
    public void testSuccessfulTryLockClearsWaitTimeouts() {
        lock.lock();

        RaftGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        final LockRegistry registry = service.getRegistryOrNull(groupId);

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
            }
        });

        lock.unlock();

        assertOpenEventually(latch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testFailedTryLockClearsWaitTimeouts() {
        lockByOtherThread(lock);

        RaftGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        LockRegistry registry = service.getRegistryOrNull(groupId);

        long fence = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);

        assertEquals(RaftLockService.INVALID_FENCE, fence);
        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testDestroyClearsWaitTimeouts() {
        lockByOtherThread(lock);

        RaftGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        final LockRegistry registry = service.getRegistryOrNull(groupId);

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
            }
        });

        lock.destroy();

        assertTrue(registry.getWaitTimeouts().isEmpty());
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

        final RaftGroupId groupId = this.lock.getGroupId();

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
        getRaftService(newInstance).triggerRaftMemberPromotion().get();
        getRaftService(newInstance).triggerRebalanceRaftGroups().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftNodeImpl raftNode = getRaftNode(newInstance, groupId);
                assertNotNull(raftNode);
                assertTrue(getSnapshotEntry(raftNode).index() > 0);

                RaftLockService service = getNodeEngineImpl(newInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getWaitTimeouts().isEmpty());
                RaftLockOwnershipState ownership = registry.getLockOwnershipState(name);
                assertTrue(ownership.isLocked());
                assertTrue(ownership.getLockCount() > 0);
                assertEquals(fence, ownership.getFence());
            }
        });
    }

    @Test
    public void testInactiveSessionsAreEventuallyClosed() {
        lock.lock();

        final RaftGroupId groupId = lock.getGroupId();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    SessionService sessionService = getNodeEngineImpl(instance).getService(SessionService.SERVICE_NAME);
                    assertFalse(sessionService.getAllSessions(groupId).isEmpty());
                }
            }
        });

        lock.forceUnlock();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    SessionService service = getNodeEngineImpl(instance).getService(SessionService.SERVICE_NAME);
                    assertTrue(service.getAllSessions(groupId).isEmpty());
                }

                SessionManagerService service = getNodeEngineImpl(lockInstance).getService(SessionManagerService.SERVICE_NAME);
                assertEquals(NO_SESSION_ID, service.getSession(groupId));
            }
        });
    }

    @Test
    public void testActiveSessionIsNotClosedWhenLockIsHeld() {
        lock.lock();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    SessionService sessionService = getNodeEngineImpl(instance).getService(SessionService.SERVICE_NAME);
                    assertFalse(sessionService.getAllSessions(lock.getGroupId()).isEmpty());
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    SessionService sessionService = getNodeEngineImpl(instance).getService(SessionService.SERVICE_NAME);
                    assertFalse(sessionService.getAllSessions(lock.getGroupId()).isEmpty());
                }
            }
        }, 20);
    }

    @Test
    public void testActiveSessionIsNotClosedWhenPendingWaitKey() {
        FencedLock other = null;
        for (HazelcastInstance instance : instances) {
            if (instance != lockInstance) {
                other = createLock(instance);
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
            public void run() {
                for (HazelcastInstance instance : instances) {
                    SessionService sessionService = getNodeEngineImpl(instance).getService(SessionService.SERVICE_NAME);
                    assertEquals(2, sessionService.getAllSessions(lock.getGroupId()).size());
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    SessionService sessionService = getNodeEngineImpl(instance).getService(SessionService.SERVICE_NAME);
                    assertEquals(2, sessionService.getAllSessions(lock.getGroupId()).size());
                }
            }
        }, 20);
    }

}

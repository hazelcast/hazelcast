package com.hazelcast.raft.service.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.exception.WaitKeyCancelledException;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.raft.service.lock.operation.TryLockOp;
import com.hazelcast.raft.service.lock.operation.UnlockOp;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.concurrent.lock.LockTestUtils.lockByOtherThread;
import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftLockFailureTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private HazelcastInstance lockInstance;
    private RaftLockProxy lock;
    private String name = "lock";

    @Before
    public void setup() {
        instances = newInstances(3);
        lock = createLock(name);
        assertNotNull(lock);
    }

    private RaftLockProxy createLock(String name) {
        lockInstance = instances[RandomPicker.getInt(instances.length)];
        return create(lockInstance, RaftLockService.SERVICE_NAME, name);
    }

    private AbstractSessionManager getSessionManager() {
        return getNodeEngineImpl(lockInstance).getService(SessionManagerService.SERVICE_NAME);
    }

    @Test
    public void testRetriedLockDoesNotCancelPendingLockRequest() {
        lockByOtherThread(lock);

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid));

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        }, 10);
    }

    @Test(timeout = 30000)
    public void testNewLockCancelsPendingLockRequest() {
        lockByOtherThread(lock);

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid1, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid2));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test
    public void testRetriedTryLockWithTimeoutDoesNotCancelPendingLockRequest() {
        lockByOtherThread(lock);

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertEquals(2, registry.getWaitTimeouts().size());
            }
        });
    }

    @Test(timeout = 30000)
    public void testNewTryLockWithTimeoutCancelsPendingLockRequest() {
        lockByOtherThread(lock);

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid1, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid2, MINUTES.toMillis(5)));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test
    public void testRetriedTryLockWithoutTimeoutDoesNotCancelPendingLockRequest() {
        lockByOtherThread(lock);

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid, 0));

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        }, 10);
    }

    @Test(timeout = 30000)
    public void testNewUnlockCancelsPendingLockRequest() {
        lockByOtherThread(lock);

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
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

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid)).join();
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid)).join();

        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testLockReentrantAcquireRetry() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid1)).join();
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid2)).join();
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid2)).join();

        assertEquals(2, lock.getLockCount());
    }

    @Test
    public void testRetriedUnlockIsSuccessfulAfterLockedByAnotherEndpoint() {
        lock.lock();

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new UnlockOp(name, sessionId, getThreadId(), invUid, 1)).join();

        lockByOtherThread(lock);

        invocationManager.invoke(groupId, new UnlockOp(name, sessionId, getThreadId(), invUid, 1)).join();
    }

}

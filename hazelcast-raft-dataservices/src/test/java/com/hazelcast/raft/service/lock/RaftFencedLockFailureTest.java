package com.hazelcast.raft.service.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.service.exception.WaitKeyCancelledException;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.raft.service.lock.operation.TryLockOp;
import com.hazelcast.raft.service.lock.operation.UnlockOp;
import com.hazelcast.raft.service.lock.proxy.RaftFencedLockProxy;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.InternalCompletableFuture;
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

import java.util.UUID;

import static com.hazelcast.raft.service.lock.RaftLockService.INVALID_FENCE;
import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftFencedLockFailureTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private HazelcastInstance lockInstance;
    private RaftFencedLockProxy lock;
    private String name = "lock";

    @Before
    public void setup() {
        instances = newInstances(3);
        lock = createLock();
        assertNotNull(lock);
    }

    private RaftFencedLockProxy createLock() {
        lockInstance = instances[RandomPicker.getInt(instances.length)];
        NodeEngineImpl nodeEngine = getNodeEngineImpl(lockInstance);
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

    private AbstractSessionManager getSessionManager() {
        return getNodeEngineImpl(lockInstance).getService(SessionManagerService.SERVICE_NAME);
    }

    @Test
    public void testRetriedLockDoesNotCancelPendingLockRequest() {
        lockByOtherThread();

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
        lockByOtherThread();

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
        lockByOtherThread();

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
        lockByOtherThread();

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
        lockByOtherThread();

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
        lockByOtherThread();

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

        RaftGroupId groupId = lock.getGroupId();
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

        RaftGroupId groupId = lock.getGroupId();
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

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new UnlockOp(name, sessionId, getThreadId(), invUid, 1)).join();

        lockByOtherThread();

        invocationManager.invoke(groupId, new UnlockOp(name, sessionId, getThreadId(), invUid, 1)).join();
    }

    @Test
    public void testIsLockedByCurrentThreadCallInitializesLocalLockState() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), newUnsecureUUID())).join();

        // the current thread acquired the lock once and we pretend that there was a operation timeout in lock.lock() call

        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLocalLockCount());
        assertNotEquals(INVALID_FENCE, lock.getLocalLockFence());
    }

    @Test
    public void testIsLockedByCurrentThreadCallInitializesLocalReentrantLockState() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), newUnsecureUUID())).join();
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), newUnsecureUUID())).join();

        // the current thread acquired the lock twice and we pretend that both lock.lock() calls failed with operation timeout

        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLocalLockCount());
        assertNotEquals(INVALID_FENCE, lock.getLocalLockFence());
    }

    @Test
    public void testLockCallInitializesLocalReentrantLockState() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), newUnsecureUUID())).join();
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), newUnsecureUUID())).join();

        lock.lock();

        assertEquals(3, lock.getLocalLockCount());
        assertNotEquals(INVALID_FENCE, lock.getLocalLockFence());
    }

    @Test
    public void testUnlockReleasesObservedAcquiresOneByOne() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), newUnsecureUUID())).join();

        lock.lock();

        lock.unlock();

        assertEquals(1, lock.getLocalLockCount());
        assertNotEquals(INVALID_FENCE, lock.getLocalLockFence());

        lock.unlock();

        assertEquals(0, lock.getLocalLockCount());
        assertEquals(INVALID_FENCE, lock.getLocalLockFence());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertFalse(registry.getLockOwnershipState(name).isLocked());
            }
        });
    }

    @Test
    public void testUnlockReleasesNotObservedAcquiresAllAtOnce() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), newUnsecureUUID())).join();
        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), newUnsecureUUID())).join();

        lock.unlock();

        assertFalse(lock.isLockedByCurrentThread());
        assertEquals(0, lock.getLocalLockCount());
        assertEquals(INVALID_FENCE, lock.getLocalLockFence());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertFalse(registry.getLockOwnershipState(name).isLocked());
            }
        });
    }

    public void lockByOtherThread() {
        Thread t = new Thread() {
            public void run() {
                try {
                    lock.lock();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}

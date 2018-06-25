package com.hazelcast.raft.service.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.session.SessionService;
import com.hazelcast.raft.service.lock.proxy.RaftFencedLockProxy;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftFencedLockBasicTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    protected HazelcastInstance lockInstance;
    private FencedLock lock;

    @Before
    public void setup() {
        instances = createInstances();
        lock = createLock("lock");
        assertNotNull(lock);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(3);
    }

    protected FencedLock createLock(String name) {
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

    protected AbstractSessionManager getSessionManager(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(SessionManagerService.SERVICE_NAME);
    }

    @Test
    public void testLock_whenNotLocked() {
        long fence = lock.lockAndGetFence();
        assertTrue(fence > 0);
        assertTrue(this.lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
        assertEquals(fence, lock.getFence());
    }

    @Test
    public void testLock_whenLockedBySelf() {
        long fence = lock.lockAndGetFence();
        assertTrue(fence > 0);

        long newFence = lock.lockAndGetFence();
        assertEquals(fence, newFence);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test
    public void testLock_whenLockedByOther() throws InterruptedException {
        long fence = lock.lockAndGetFence();
        assertTrue(fence > 0);
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
        assertTrue(lock.isLockedByCurrentThread());

        final CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread() {
            public void run() {
                lock.lock();
                latch.countDown();
            }
        };

        t.start();
        assertFalse(latch.await(3000, TimeUnit.MILLISECONDS));
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
    public void testUnlock_whenLockedBySelf() {
        lock.lock();

        lock.unlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test
    public void testUnlock_whenReentrantlyLockedBySelf() {
        long fence = lock.lockAndGetFence();
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
        lock.unlock();

        final AtomicReference<Long> newFenceRef = new AtomicReference<Long>();
        spawn(new Runnable() {
            @Override
            public void run() {
                long newFence = lock.lockAndGetFence();
                newFenceRef.set(newFence);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(newFenceRef.get());
            }
        });

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
        final long fence = lock.lockAndGetFence();
        final AtomicReference<Long> newFenceRef = new AtomicReference<Long>();
        spawn(new Runnable() {
            @Override
            public void run() {
                long newFence = lock.lockAndGetFence();
                newFenceRef.set(newFence);
            }
        });

        lock.unlock();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(newFenceRef.get());
            }
        });

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

        assertTrue(fence > 0);
        assertEquals(fence, lock.getFence());
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testTryLock_whenLockedBySelf() {
        long fence = lock.lockAndGetFence();
        assertTrue(fence > 0);

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

        assertEquals(0, fence);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testTryLockTimeout() {
        long fence = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);

        assertTrue(fence > 0);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testTryLockTimeout_whenLockedBySelf() {
        long fence = lock.lockAndGetFence();
        assertTrue(fence > 0);

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

        assertEquals(0, fence);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockLongTimeout_whenLockedByOther() {
        lockByOtherThread(lock);

        long fence = lock.tryLockAndGetFence(RaftLockService.WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS + 1, TimeUnit.MILLISECONDS);

        assertEquals(0, fence);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void test_ReentrantLockFails_whenSessionClosed() throws ExecutionException, InterruptedException {
        long fence = lock.lockAndGetFence();
        assertTrue(fence > 0);

        final AbstractSessionManager sessionManager = getSessionManager(lockInstance);
        final RaftGroupId groupId = lock.getGroupId();
        final long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(AbstractSessionManager.NO_SESSION_ID, sessionId);

        closeSession(instances[0], groupId, sessionId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotEquals(sessionId, sessionManager.getSession(groupId));
            }
        });

        try {
            lock.lock();
        } catch (IllegalMonitorStateException ignored) {
        }
    }

    @Test
    public void test_ReentrantTryLockFails_whenSessionClosed() throws ExecutionException, InterruptedException {
        long fence = lock.lockAndGetFence();
        assertTrue(fence > 0);

        final AbstractSessionManager sessionManager = getSessionManager(lockInstance);
        final RaftGroupId groupId = lock.getGroupId();
        final long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(AbstractSessionManager.NO_SESSION_ID, sessionId);

        closeSession(instances[0], groupId, sessionId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotEquals(sessionId, sessionManager.getSession(groupId));
            }
        });

        try {
            lock.tryLock();
        } catch (IllegalMonitorStateException ignored) {
        }
    }

    @Test
    public void test_ReentrantTryLockWithTimeoutFails_whenSessionClosed() throws ExecutionException, InterruptedException {
        long fence = lock.lockAndGetFence();
        assertTrue(fence > 0);

        final AbstractSessionManager sessionManager = getSessionManager(lockInstance);
        final RaftGroupId groupId = lock.getGroupId();
        final long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(AbstractSessionManager.NO_SESSION_ID, sessionId);

        closeSession(instances[0], groupId, sessionId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotEquals(sessionId, sessionManager.getSession(groupId));
            }
        });

        try {
            lock.tryLock(1, TimeUnit.SECONDS);
        } catch (IllegalMonitorStateException ignored) {
        }
    }

    @Test
    public void test_forceUnlock_whenLocked() {
        long fence = lock.lockAndGetFence();
        assertTrue(fence > 0);

        lock.forceUnlock();

        assertFalse(lock.isLockedByCurrentThread());
        assertFalse(lock.isLocked());
    }

    @Test(timeout = 60000)
    public void test_forceUnlock_byOtherEndpoint() {
        long fence = lock.lockAndGetFence();
        assertTrue(fence > 0);

        final CountDownLatch latch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                lock.forceUnlock();
                latch.countDown();
            }
        });

        assertOpenEventually(latch);
    }

    @Test
    public void test_forceUnlock_whenNotLocked() {
        try {
            lock.forceUnlock();
            fail();
        } catch (IllegalMonitorStateException ignored) {
        }

        assertFalse(lock.isLockedByCurrentThread());
        assertFalse(lock.isLocked());
    }

    @Test(timeout = 60000)
    public void test_reentrantLock_whenForceUnlockedByOtherEndpoint() throws ExecutionException, InterruptedException {
        long fence1 = lock.lockAndGetFence();
        assertTrue(fence1 > 0);

        spawn(new Runnable() {
            @Override
            public void run() {
                lock.forceUnlock();
            }
        }).get();

        lock.lock();

        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void test_failedTryLock_doesNotAcquireSession() {
        lockByOtherThread(lock);

        final AbstractSessionManager sessionManager = getSessionManager(lockInstance);
        final RaftGroupId groupId = lock.getGroupId();
        final long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(AbstractSessionManager.NO_SESSION_ID, sessionId);
        assertEquals(1, sessionManager.getSessionAcquireCount(groupId, sessionId));

        long fence = lock.tryLockAndGetFence();
        assertEquals(RaftLockService.INVALID_FENCE, fence);
        assertEquals(1, sessionManager.getSessionAcquireCount(groupId, sessionId));
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void test_destroy() {
        lock.destroy();

        lock.lock();
    }

    private void closeSession(HazelcastInstance instance, RaftGroupId groupId, long sessionId) throws ExecutionException, InterruptedException {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        SessionService service = nodeEngine.getService(SessionService.SERVICE_NAME);
        service.forceCloseSession(groupId, sessionId).get();
    }

    static void lockByOtherThread(final FencedLock lock) {
        try {
            spawn(new Runnable() {
                @Override
                public void run() {
                    lock.lock();
                }
            }).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

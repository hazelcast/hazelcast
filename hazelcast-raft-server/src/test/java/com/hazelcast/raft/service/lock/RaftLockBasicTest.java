package com.hazelcast.raft.service.lock;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.nio.Address;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.concurrent.lock.LockTestUtils.lockByOtherThread;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftLockBasicTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private ILock lock;
    private String name = "lock";
    private int groupSize = 3;

    @Before
    public void setup() {
        Address[] raftAddresses = createAddresses(groupSize);
        instances = newInstances(raftAddresses, groupSize, 0);

        lock = RaftLockProxy.create(instances[RandomPicker.getInt(instances.length)], name);
        assertNotNull(lock);
    }

    @Test
    public void testLock_whenNotLocked() {
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testLock_whenLockedBySelf() {
        lock.lock();
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test
    public void testLock_whenLockedByOther() throws InterruptedException {
        lock.lock();
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

    @Test
    public void testUnlock_whenLockedBySelf() {
        lock.lock();

        lock.unlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test
    public void testUnlock_whenReentrantlyLockedBySelf() {
        lock.lock();
        lock.lock();

        lock.unlock();

        assertTrue(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testLock_Unlock_thenLock() throws Exception {
        lock.lock();
        lock.unlock();

        spawn(new Runnable() {
            @Override
            public void run() {
                lock.lock();
            }
        }).get(1, TimeUnit.MINUTES);

        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testUnlock_whenPendingLockOfOtherThread() throws InterruptedException {
        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                latch.countDown();

            }
        });
        thread.start();

        sleepSeconds(1);
        lock.unlock();
        latch.await();

        assertTrue(lock.isLocked());
        assertFalse(lock.isLockedByCurrentThread());
    }

    @Test
    public void testUnlock_whenLockedByOther() {
        lockByOtherThread(lock);

        try {
            lock.unlock();
            fail();
        } catch (IllegalMonitorStateException expected) {
        }

        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenNotLocked() {
        boolean result = lock.tryLock();

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenLockedBySelf() {
        lock.lock();

        boolean result = lock.tryLock();

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenLockedByOther() {
        lockByOtherThread(lock);

        boolean result = lock.tryLock();

        assertFalse(result);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Override
    protected Config createConfig(Address[] raftAddresses, int metadataGroupSize) {
        ServiceConfig lockServiceConfig = new ServiceConfig().setEnabled(true)
                .setName(RaftLockService.SERVICE_NAME).setClassName(RaftLockService.class.getName());

        Config config = super.createConfig(raftAddresses, metadataGroupSize);
        config.getServicesConfig().addServiceConfig(lockServiceConfig);

        RaftLockConfig lockConfig = new RaftLockConfig(name, new RaftGroupConfig(name, groupSize));
        config.addRaftLockConfig(lockConfig);
        return config;
    }
}

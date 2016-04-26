package com.hazelcast.concurrent.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ILock;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LockAdvancedTest extends HazelcastTestSupport {

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void testShutDownNodeWhenOtherWaitingOnLockLocalKey() throws InterruptedException {
        testShutDownNodeWhenOtherWaitingOnLock(true);
    }

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void testShutDownNodeWhenOtherWaitingOnLockRemoteKey() throws InterruptedException {
        testShutDownNodeWhenOtherWaitingOnLock(false);
    }

    private void testShutDownNodeWhenOtherWaitingOnLock(boolean localKey) throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();
        warmUpPartitions(instance2, instance);

        final String key;
        if (localKey) {
            key = generateKeyOwnedBy(instance);
        } else {
            key = generateKeyNotOwnedBy(instance);
        }

        final ILock lock = instance.getLock(key);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                lock.lock();
            }
        });
        thread.start();
        thread.join();
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                instance.shutdown();
            }
        }).start();
        lock.lock();
    }

    @Test(timeout = 100000)
    public void testLockEvictionLocalKey() throws Exception {
        testLockEviction(true);
    }

    @Test(timeout = 100000)
    public void testLockEvictionRemoteKey() throws Exception {
        testLockEviction(false);
    }

    private void testLockEviction(boolean localKey) throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        warmUpPartitions(instance2, instance1);

        final String key;
        if (localKey) {
            key = generateKeyOwnedBy(instance1);
        } else {
            key = generateKeyNotOwnedBy(instance1);
        }

        final ILock lock = instance1.getLock(key);
        lock.lock(10, TimeUnit.SECONDS);
        assertTrue(lock.getRemainingLeaseTime() > 0);
        assertTrue(lock.isLocked());

        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                final ILock lock = instance2.getLock(key);
                lock.lock();
                latch.countDown();

            }
        });
        t.start();
        assertTrue(latch.await(30, TimeUnit.SECONDS));
    }


    /**
     * Test for issue #39
     */
    @Test
    public void testIsLocked() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();
        final String key = "testLockIsLocked";
        final ILock lock = h1.getLock(key);
        final ILock lock2 = h2.getLock(key);

        assertFalse(lock.isLocked());
        assertFalse(lock2.isLocked());
        lock.lock();
        assertTrue(lock.isLocked());
        assertTrue(lock2.isLocked());

        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        Thread thread = new Thread(new Runnable() {
            public void run() {
                ILock lock3 = h3.getLock(key);
                assertTrue(lock3.isLocked());
                try {
                    latch2.countDown();
                    while (lock3.isLocked()) {
                        Thread.sleep(100);
                    }
                    latch.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();
        latch2.await(3, TimeUnit.SECONDS);
        Thread.sleep(500);
        lock.unlock();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    //todo:   what does isLocked2 test?
    @Test(timeout = 60000)
    public void testIsLocked2() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        final String key = randomString();

        final ILock lock = instance1.getLock(key);
        lock.lock();
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());

        assertTrue(lock.tryLock());
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());

        final AtomicBoolean result = new AtomicBoolean();
        final Thread thread = new Thread() {
            public void run() {
                result.set(lock.isLockedByCurrentThread());
            }
        };
        thread.start();
        thread.join();
        assertFalse(result.get());

        lock.unlock();
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());
    }

    @Test(timeout = 60000)
    public void testLockInterruption() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "5000");
        final HazelcastInstance hz = createHazelcastInstance(config);

        final Lock lock = hz.getLock("testLockInterruption2");
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    lock.tryLock(60, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    latch.countDown();
                }
            }
        });
        lock.lock();
        t.start();
        Thread.sleep(2000);
        t.interrupt();
        assertTrue("tryLock() is not interrupted!", latch.await(30, TimeUnit.SECONDS));
        lock.unlock();
        assertTrue("Could not acquire lock!", lock.tryLock());
    }


    // ====================== tests to make sure the lock can deal with cluster member failure ====================

    @Test(timeout = 100000)
    public void testLockOwnerDies() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance lockOwner = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();

        final String name = randomString();
        final ILock lock = lockOwner.getLock(name);
        lock.lock();
        assertTrue(lock.isLocked());
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                final ILock lock = instance1.getLock(name);
                lock.lock();
                latch.countDown();

            }
        });
        t.start();
        lockOwner.shutdown();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 100000)
    public void testKeyOwnerDies() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final HazelcastInstance keyOwner = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        warmUpPartitions(keyOwner, instance1, instance2);
        final String key = generateKeyOwnedBy(keyOwner);
        final ILock lock1 = instance1.getLock(key);
        lock1.lock();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                final ILock lock = instance2.getLock(key);
                lock.lock();
                latch.countDown();
            }
        }).start();

        Thread.sleep(1000);
        keyOwner.shutdown();
        assertTrue(lock1.isLocked());
        assertTrue(lock1.isLockedByCurrentThread());
        assertTrue(lock1.tryLock());
        lock1.unlock();
        lock1.unlock();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 100000)
    public void testScheduledLockActionForDeadMember() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final ILock lock1 = h1.getLock("default");
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final ILock lock2 = h2.getLock("default");

        assertTrue(lock1.tryLock());

        final AtomicBoolean error = new AtomicBoolean(false);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    lock2.lock();
                    error.set(true);
                } catch (Throwable ignored) {
                }
            }
        });
        thread.start();
        Thread.sleep(5000);

        assertTrue(lock1.isLocked());
        h2.shutdown();
        thread.join(10000);
        assertFalse(thread.isAlive());
        assertFalse(error.get());

        assertTrue(lock1.isLocked());
        lock1.unlock();
        assertFalse(lock1.isLocked());
        assertTrue(lock1.tryLock());
    }

    @Test
    public void testLockInterruptibly() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "5000");
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        final ILock lock = h1.getLock(randomString());
        final CountDownLatch latch = new CountDownLatch(1);
        lock.lock();
        Thread t = new Thread() {
            public void run() {
                try {
                    lock.lockInterruptibly();
                } catch (InterruptedException e) {
                    latch.countDown();
                }
            }
        };
        t.start();
        sleepMillis(5000);
        t.interrupt();
        assertTrue(latch.await(15, TimeUnit.SECONDS));
    }

    @Test
    public void testLockLeaseTime_whenKeyOwnerMemberDies() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz1, hz2);

        String key = generateKeyOwnedBy(hz1);
        final ILock lock = hz2.getLock(key);
        lock.lock(3, TimeUnit.SECONDS);

        terminateInstance(hz1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse("Lock should be released after lease expires!", lock.isLocked());
            }
        }, 30);
    }

    @Test
    public void testMaxLockLeaseTime() {
        Config config = new Config();
        config.setProperty(GroupProperty.LOCK_MAX_LEASE_TIME_SECONDS.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        final ILock lock = hz.getLock(randomName());

        lock.lock();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse("Lock should be released after lease expires!", lock.isLocked());
            }
        }, 30);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLockFail_whenGreaterThanMaxLeaseTimeUsed() {
        Config config = new Config();
        config.setProperty(GroupProperty.LOCK_MAX_LEASE_TIME_SECONDS.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        ILock lock = hz.getLock(randomName());

        lock.lock(10, TimeUnit.SECONDS);
    }

    @Test
    public void testLockCleanup_whenInvokingMemberDies() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz = factory.newHazelcastInstance();


        HazelcastInstance hz2 = factory.newHazelcastInstance();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz2);
        InternalOperationService operationService = getOperationService(hz2);
        warmUpPartitions(hz2);

        String name = randomNameOwnedBy(hz);
        Data key = nodeEngine.toData(name);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        operationService.invokeOnPartition(LockService.SERVICE_NAME, new SlowLockOperation(name, key, 2000),
                partitionId);

        sleepMillis(500);
        terminateInstance(hz2);

        final ILock lock = hz.getLock(name);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse("Lock owned by dead member should have been released!", lock.isLocked());
            }
        }, 30);
    }

    private static class SlowLockOperation extends AbstractOperation {

        Data key;
        ObjectNamespace ns;
        long sleepMillis;

        public SlowLockOperation() {
        }

        public SlowLockOperation(String name, Data key, long sleepMillis) {
            this.key = key;
            this.ns = new InternalLockNamespace(name);
            this.sleepMillis = sleepMillis;
        }

        protected final LockStoreImpl getLockStore() {
            LockServiceImpl service = getService();
            return service.getLockStore(getPartitionId(), ns);
        }

        @Override
        public void run() throws Exception {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
            getLockStore().lock(key, getCallerUuid(), 1, 1, -1);
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(sleepMillis);
            out.writeByteArray(key.toByteArray());
            out.writeUTF(ns.getObjectName());
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            sleepMillis = in.readLong();
            key = new HeapData(in.readByteArray());
            ns = new InternalLockNamespace(in.readUTF());
        }
    }

}

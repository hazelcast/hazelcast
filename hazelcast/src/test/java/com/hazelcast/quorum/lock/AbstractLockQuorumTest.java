package com.hazelcast.quorum.lock;

import com.hazelcast.config.LockConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.util.EmptyStatement;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;

public abstract class AbstractLockQuorumTest {
    protected static PartitionedCluster cluster;
    private static ILock l1;
    private static ILock l2;
    private static ILock l3;
    static ILock l4;
    static ILock l5;
    private static final String LOCK_NAME_PREFIX = "lock";
    protected static final String LOCK_NAME = LOCK_NAME_PREFIX + randomString();
    private static final String QUORUM_ID = "threeNodeQuorumRule";


    protected static void initializeFiveMemberCluster(QuorumType type, int quorumSize) throws InterruptedException {
        final QuorumConfig quorumConfig = new QuorumConfig()
                .setName(QUORUM_ID)
                .setType(type)
                .setEnabled(true)
                .setSize(quorumSize);
        final LockConfig lockConfig = new LockConfig(LOCK_NAME_PREFIX + "*").setQuorumName(QUORUM_ID);
        cluster = new PartitionedCluster(new TestHazelcastInstanceFactory());
        cluster.createFiveMemberCluster(lockConfig, quorumConfig);

        l1 = getLock(cluster.h1);
        l2 = getLock(cluster.h2);
        l3 = getLock(cluster.h3);
        l4 = getLock(cluster.h4);
        l5 = getLock(cluster.h5);
    }

    protected static <E> ILock getLock(HazelcastInstance instance) {
        return instance.getLock(LOCK_NAME);
    }

    @Test
    public void testOperationsSuccessfulWhenQuorumSizeMet() throws Exception {
        l1.lock();
        l2.getRemainingLeaseTime();
        l1.unlock();

        l2.lock();
        l3.forceUnlock();

        l3.lock();
        l3.isLocked();
        l3.getLockCount();
        l2.isLockedByCurrentThread();
        l3.unlock();

        l1.tryLock();
        l1.unlock();

        testCondition(l1);
    }

    void testCondition(ILock lock) throws InterruptedException {
        final CountDownLatch signalArrived = new CountDownLatch(1);
        final ICondition cond = lock.newCondition("condition");
        await(lock, cond, signalArrived);

        TimeUnit.SECONDS.sleep(1);
        lock.lock();
        cond.signal();
        lock.unlock();
        assertOpenEventually(signalArrived);
    }

    private void await(final ILock lock, final ICondition cond, final CountDownLatch signalArrived) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.lock();
                    cond.await();
                    signalArrived.countDown();
                    lock.unlock();
                } catch (InterruptedException e) {
                    EmptyStatement.ignore(e);
                }
            }
        }).start();
    }
}

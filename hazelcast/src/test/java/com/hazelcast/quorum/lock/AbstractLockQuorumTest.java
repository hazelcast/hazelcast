/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;

public abstract class AbstractLockQuorumTest {

    private static final String LOCK_NAME_PREFIX = "lock";
    private static final String LOCK_NAME = LOCK_NAME_PREFIX + randomString();

    protected static PartitionedCluster cluster;

    static ILock l1;
    static ILock l2;
    static ILock l3;
    static ILock l4;
    static ILock l5;

    static void initializeFiveMemberCluster(QuorumType type, int quorumSize) {
        QuorumConfig quorumConfig = new QuorumConfig()
                .setName(QUORUM_ID)
                .setType(type)
                .setEnabled(true)
                .setSize(quorumSize);
        LockConfig lockConfig = new LockConfig(LOCK_NAME_PREFIX + "*").setQuorumName(QUORUM_ID);
        cluster = new PartitionedCluster(new TestHazelcastInstanceFactory());
        cluster.createFiveMemberCluster(lockConfig, quorumConfig);

        l1 = getLock(cluster.h1);
        l2 = getLock(cluster.h2);
        l3 = getLock(cluster.h3);
        l4 = getLock(cluster.h4);
        l5 = getLock(cluster.h5);
    }

    protected static ILock getLock(HazelcastInstance instance) {
        return instance.getLock(LOCK_NAME);
    }

    @Test
    public void testOperationsSuccessfulWhenQuorumSizeMet() {
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

    void testCondition(ILock lock) {
        CountDownLatch signalArrived = new CountDownLatch(1);
        ICondition cond = lock.newCondition("condition");
        await(lock, cond, signalArrived);

        sleepSeconds(1);
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

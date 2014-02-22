/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author mdogan 9/16/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class InvocationTest extends HazelcastTestSupport {

    @Test
    public void whenPartitionTargetMemberDiesThenOperationSendToNewPartitionOwner() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();
        warmUpPartitions(local, remote);

        Node localNode = getNode(local);
        OperationService service = localNode.nodeEngine.getOperationService();
        Operation op = new PartitionTargetOperation();
        String partitionKey = generateKeyOwnedBy(remote);
        int partitionid = localNode.nodeEngine.getPartitionService().getPartitionId(partitionKey);
        Future f = service.createInvocationBuilder(null, op, partitionid).setCallTimeout(30000).build().invoke();
        sleepSeconds(1);

        remote.shutdown();

        //the get should work without a problem because the operation should be re-targeted at the newest owner
        //for that given partition
        f.get();
    }

    @Test
    public void whenTargetMemberDiesThenOperationAbortedWithMembersLeftException() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();
        warmUpPartitions(local, remote);

        OperationService service = getNode(local).nodeEngine.getOperationService();
        Operation op = new TargetOperation();
        Address address = new Address(remote.getCluster().getLocalMember().getInetSocketAddress());
        Future f = service.createInvocationBuilder(null, op, address).build().invoke();
        sleepSeconds(1);

        remote.shutdown();

        try {
            f.get();
            fail();
        } catch (MemberLeftException expected) {

        }
    }

    private static class TargetOperation extends AbstractOperation {
        public void run() throws InterruptedException {
            Thread.sleep(5000);
        }
    }


    private static class PartitionTargetOperation extends AbstractOperation implements PartitionAwareOperation {

        public void run() throws InterruptedException {
            Thread.sleep(5000);
        }
    }

    @Test
    public void testTimeoutDuringBlocking() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance(new Config());
        final IQueue<Object> q = hz.getQueue("queue");

        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    Object poll = q.poll(1, TimeUnit.SECONDS);
                    assertNull(poll);
                    latch.countDown();
                    ;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        assertTrue(latch.await(1, TimeUnit.MINUTES));
    }

    @Test
    public void testInterruptionDuringBlockingOp1() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance(new Config());
        final IQueue<Object> q = hz.getQueue("queue");

        final AtomicBoolean interruptedFlag = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new OpThread(interruptedFlag, latch) {
            protected void doOp() {
                try {
                    assertNotNull(q.poll(1, TimeUnit.MINUTES));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        q.offer("new item!");

        assertTrue(latch.await(1, TimeUnit.MINUTES));
        assertTrue(interruptedFlag.get());
    }

    @Test
    public void testWaitingInfinitelyForTryLock() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "2000");
        final HazelcastInstance hz = factory.newHazelcastInstance(config);
        final CountDownLatch latch = new CountDownLatch(1);

        hz.getLock("testWaitingInfinitelyForTryLock").lock();

        new Thread() {
            public void run() {
                try {
                    hz.getLock("testWaitingInfinitelyForTryLock").tryLock(5, TimeUnit.SECONDS);
                    latch.countDown();
                } catch (Exception ignored) {
                }
            }
        }.start();

        assertTrue(latch.await(15, TimeUnit.SECONDS));

    }

    @Test
    public void testInterruptionDuringBlockingOp2() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance(new Config());
        final ILock lock = hz.getLock("lock");
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());

        final AtomicBoolean interruptedFlag = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new OpThread(interruptedFlag, latch) {
            protected void doOp() {
                try {
                    assertTrue(lock.tryLock(1, TimeUnit.MINUTES));
                    assertTrue(lock.isLockedByCurrentThread());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        lock.unlock();

        assertTrue(latch.await(1, TimeUnit.MINUTES));
        assertTrue(interruptedFlag.get());
    }

    private abstract class OpThread extends Thread {
        final AtomicBoolean interruptedFlag;
        final CountDownLatch latch;

        protected OpThread(AtomicBoolean interruptedFlag, CountDownLatch latch) {
            this.interruptedFlag = interruptedFlag;
            this.latch = latch;
        }

        public void run() {
            doOp();
            interruptedFlag.set(isInterrupted());
            try {
                sleep(1000 * 60);
            } catch (InterruptedException e) {
                latch.countDown();
            }
        }

        protected abstract void doOp();
    }
}

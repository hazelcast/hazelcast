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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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
        int partitionId = localNode.nodeEngine.getPartitionService().getPartitionId(partitionKey);
        Future f = service.createInvocationBuilder(null, op, partitionId).setCallTimeout(30000).invoke();
        sleepSeconds(1);

        remote.shutdown();

        //the get should work without a problem because the operation should be re-targeted at the newest owner
        //for that given partition
        f.get();
    }

    @Repeat(100)
    @Test
    public void whenTargetMemberDiesThenOperationAbortedWithMembersLeftException() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();
        warmUpPartitions(local, remote);

        OperationService service = getNode(local).nodeEngine.getOperationService();
        Operation op = new TargetOperation();
        Address address = new Address(remote.getCluster().getLocalMember().getSocketAddress());
        Future f = service.createInvocationBuilder(null, op, address).invoke();
        sleepSeconds(1);

        remote.getLifecycleService().terminate();

        try {
            f.get();
            fail();
        } catch (MemberLeftException expected) {

        }
    }

    /**
     * Operation send to a specific member.
     */
    private static class TargetOperation extends AbstractOperation {
        public void run() throws InterruptedException {
            Thread.sleep(10000);
        }
    }

    /**
     * Operation send to a specific target partition.
     */
    private static class PartitionTargetOperation extends AbstractOperation implements PartitionAwareOperation {

        public void run() throws InterruptedException {
            Thread.sleep(5000);
        }
    }

    @Test
    public void testInterruptionDuringBlockingOp1() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        final IQueue<Object> q = hz.getQueue("queue");

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean interruptedFlag = new AtomicBoolean(false);

        OpThread thread = new OpThread("Queue Thread", latch, interruptedFlag) {
            protected void doOp() throws InterruptedException {
                q.poll(1, TimeUnit.MINUTES);
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        q.offer("new item!");

        assertTrue(latch.await(1, TimeUnit.MINUTES));

        if (thread.isInterruptionCaught()) {
            assertFalse("Thread interrupted flag should not be set!", interruptedFlag.get());
            assertFalse("Queue should not be empty!", q.isEmpty());
        } else {
            assertTrue("Thread interrupted flag should be set!", interruptedFlag.get());
            assertTrue("Queue should be empty!", q.isEmpty());
        }
    }

    @Test
    public void testWaitingIndefinitely() throws InterruptedException {
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "2000");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);

        // need to warm-up partitions,
        // since waiting for lock backup can take up to 5 seconds
        // and that may cause OperationTimeoutException with "No response for 4000 ms" error.
        warmUpPartitions(instances);

        instances[0].getLock("testWaitingIndefinitely").lock();


        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    // because max timeout=2000 we get timeout exception which we should not
                    instances[1].getLock("testWaitingIndefinitely").lock();
                    latch.countDown();
                } catch (Exception ignored) {
                }
            }
        }.start();


        // wait for enough time which is greater than max-timeout (2000)
        Thread.sleep(10000);

        instances[0].getLock("testWaitingIndefinitely").unlock();

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testWaitingInfinitelyForTryLock() throws InterruptedException {
       final Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "2000");
        final HazelcastInstance hz = createHazelcastInstance(config);
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
        HazelcastInstance hz = createHazelcastInstance();
        final ILock lock = hz.getLock("lock");
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean interruptedFlag = new AtomicBoolean(false);

        final OpThread thread = new OpThread("Lock-Thread", latch, interruptedFlag) {
            protected void doOp() throws InterruptedException {
                assertTrue(lock.tryLock(1, TimeUnit.MINUTES));
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        lock.unlock();

        assertTrue(latch.await(1, TimeUnit.MINUTES));

        if (thread.isInterruptionCaught()) {
            assertFalse("Thread interrupted flag should not be set!", interruptedFlag.get());
            assertFalse("Lock should not be in 'locked' state!", lock.isLocked());
        } else {
            assertTrue("Thread interrupted flag should be set! " + thread, interruptedFlag.get());
            assertTrue("Lock should be 'locked' state!", lock.isLocked());
        }
    }

    private abstract class OpThread extends Thread {
        final CountDownLatch latch;
        final AtomicBoolean interruptionCaught = new AtomicBoolean(false);
        final AtomicBoolean interruptedFlag;

        protected OpThread(String name, CountDownLatch latch, AtomicBoolean interruptedFlag) {
            super(name);
            this.latch = latch;
            this.interruptedFlag = interruptedFlag;
        }

        public void run() {
            try {
                doOp();
                interruptedFlag.set(isInterrupted());
            } catch (InterruptedException e) {
                interruptionCaught.set(true);
            } finally {
                latch.countDown();
            }
        }

        private boolean isInterruptionCaught() {
            return interruptionCaught.get();
        }

        protected abstract void doOp() throws InterruptedException;
    }
}

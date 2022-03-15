/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.locksupport.operations.IsLockedOperation;
import com.hazelcast.internal.locksupport.operations.LockOperation;
import com.hazelcast.internal.locksupport.operations.UnlockOperation;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_BlockingTest extends HazelcastTestSupport {

    // ============================ heartbeat timeout =============================================================================
    //
    // ===========================================================================================================================
    @Test
    public void sync_whenHeartbeatTimeout() {
        int callTimeout = 5000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(factory.getAllHazelcastInstances());

        NodeEngineImpl nodeEngine = getNodeEngineImpl(local);

        String key = generateKeyOwnedBy(remote);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        // first we execute an operation that stall the partition.
        OperationServiceImpl opService = nodeEngine.getOperationService();
        opService.invokeOnPartition(null, new SlowOperation(5 * callTimeout), partitionId);

        // then we execute a lock operation that won't be executed because the partition is blocked.
        LockOperation op = new LockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), 1, -1, -1);
        InternalCompletableFuture<Object> future = opService.createInvocationBuilder(null, op, partitionId)
                .setCallTimeout(callTimeout)
                .invoke();

        try {
            future.joinInternal();
            fail("Invocation should failed with timeout!");
        } catch (OperationTimeoutException expected) {
            ignore(expected);
        }

        IsLockedOperation isLockedOperation = new IsLockedOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), 1);
        Boolean isLocked = (Boolean) opService.createInvocationBuilder(
                null, isLockedOperation, partitionId)
                .setCallTimeout(10 * callTimeout)
                .invoke()
                .join();
        assertFalse(isLocked);
    }

    /**
     * Tests that an ExecutionCallback is called when an OperationTimeoutException happens. This is a problem in 3.6
     * since async calls don't get the same timeout logic.
     */
    @Test
    public void async_whenHeartbeatTimeout() {
        int callTimeout = 5000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(factory.getAllHazelcastInstances());

        NodeEngineImpl nodeEngine = getNodeEngineImpl(local);

        String key = generateKeyOwnedBy(remote);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        // first we execute an operation that stall the partition.
        OperationServiceImpl opService = nodeEngine.getOperationService();
        opService.invokeOnPartition(null, new SlowOperation(5 * callTimeout), partitionId);

        // then we execute a lock operation that won't be executed because the partition is blocked.
        LockOperation op = new LockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), 1, -1, -1);
        InternalCompletableFuture<Object> future = opService.createInvocationBuilder(null, op, partitionId)
                .setCallTimeout(callTimeout)
                .invoke();

        // then we register our callback
        final BiConsumer<Object, Throwable> callback = getExecutionCallbackMock();
        future.whenCompleteAsync(callback);

        // and we eventually expect to fail with an OperationTimeoutException
        assertFailsEventuallyWithOperationTimeoutException(callback);
    }

    // ====================================================================
    //
    // ====================================================================
    @Test
    public void sync_whenOperationTimeout() {
        int callTimeout = 5000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(factory.getAllHazelcastInstances());

        NodeEngineImpl nodeEngine = getNodeEngineImpl(local);

        String key = generateKeyOwnedBy(remote);
        ObjectNamespace namespace = new DistributedObjectNamespace(SERVICE_NAME, key);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        // first we lock the lock by another thread
        OperationServiceImpl opService = nodeEngine.getOperationService();
        int otherThreadId = 2;
        opService.invokeOnPartition(
                new LockOperation(namespace, nodeEngine.toData(key), otherThreadId, -1, -1)
                        .setPartitionId(partitionId))
                .join();

        // then we execute a lock operation that won't be executed because lock is already acquired
        // we are going to do some waiting (3x call timeout)
        int threadId = 1;
        Operation op = new LockOperation(namespace, nodeEngine.toData(key), threadId, -1, 3 * callTimeout)
                .setPartitionId(partitionId);
        final InternalCompletableFuture<Object> future = opService.invokeOnPartition(op);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(future.isDone());
            }
        });

        assertEquals(Boolean.FALSE, future.join());
    }

    @Test
    public void async_whenOperationTimeout() {
        int callTimeout = 5000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(factory.getAllHazelcastInstances());

        NodeEngineImpl nodeEngine = getNodeEngineImpl(local);

        String key = generateKeyOwnedBy(remote);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        // first we lock the lock by another thread
        OperationServiceImpl opService = nodeEngine.getOperationService();
        int otherThreadId = 2;
        opService.invokeOnPartition(
                new LockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), otherThreadId, -1, -1)
                        .setPartitionId(partitionId))
                .join();

        // then we execute a lock operation that won't be executed because lock is already acquired
        // we are going to do some waiting (3x call timeout)
        int threadId = 1;
        Operation op = new LockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), threadId, -1, 3 * callTimeout)
                .setPartitionId(partitionId);
        final InternalCompletableFuture<Object> future = opService.invokeOnPartition(op);

        final BiConsumer<Object, Throwable> callback = getExecutionCallbackMock();
        future.whenCompleteAsync(callback);

        assertTrueEventually(() -> verify(callback).accept(Boolean.FALSE, null));
    }

    /**
     * Checks if a get with a timeout is called, and the timeout expires, that we get a TimeoutException.
     */
    @Test
    public void sync_whenGetTimeout() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(factory.getAllHazelcastInstances());

        NodeEngineImpl nodeEngine = getNodeEngineImpl(local);

        String key = generateKeyOwnedBy(remote);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        // first we execute an operation that stall the partition
        OperationServiceImpl opService = nodeEngine.getOperationService();
        opService.invokeOnPartition(null, new SlowOperation(SECONDS.toMillis(5)), partitionId);

        // then we execute a lock operation that won't be executed because the partition is blocked.
        LockOperation op = new LockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), 1, -1, -1);
        InternalCompletableFuture<Object> future = opService.createInvocationBuilder(null, op, partitionId)
                .invoke();

        // we do a get with a very short timeout; so we should get a TimeoutException
        try {
            future.get(1, SECONDS);
            fail();
        } catch (TimeoutException expected) {
            ignore(expected);
        }

        //if we do a get with a long enough timeout, the call will complete without a problem
        Object result = future.get(60, SECONDS);
        assertEquals(Boolean.TRUE, result);
    }

    private void assertFailsEventuallyWithOperationTimeoutException(final BiConsumer callback) {
        assertTrueEventually(() -> {
            ArgumentCaptor<Throwable> argument = ArgumentCaptor.forClass(Throwable.class);
            verify(callback).accept(isNull(), argument.capture());

            assertInstanceOf(OperationTimeoutException.class, argument.getValue());
        });
    }

    /**
     * Tests if the future on a blocking operation can be shared by multiple threads. This tests fails in 3.6 because
     * only 1 thread will be able to swap out CONTINUE_WAIT and all other threads will fail with an OperationTimeoutExcepyion
     */
    @Test
    public void sync_whenManyGettersAndLotsOfWaiting() throws Exception {
        int callTimeout = 10000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(local);

        String key = generateKeyOwnedBy(remote);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        // first we execute an operation that stall the partition
        OperationServiceImpl opService = nodeEngine.getOperationService();

        // first we are going to lock
        int otherThreadId = 1;
        LockOperation otherOp = new LockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), otherThreadId, -1, -1);
        opService.createInvocationBuilder(null, otherOp, partitionId)
                .setCallTimeout(callTimeout)
                .invoke()
                .join();

        // then we are going to send the invocation and share the future by many threads
        int thisThreadId = 2;
        LockOperation thisOp = new LockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), thisThreadId, -1, -1);
        final InternalCompletableFuture<Object> future = opService.createInvocationBuilder(null, thisOp, partitionId)
                .setCallTimeout(callTimeout)
                .invoke();
        // now we are going to do a get on the future by a whole bunch of threads
        final List<Future> futures = new LinkedList<Future>();
        for (int k = 0; k < 10; k++) {
            futures.add(spawn(new Callable() {
                @Override
                public Object call() throws Exception {
                    return future.join();
                }
            }));
        }

        // lets do a very long wait so that the heartbeat/retrying mechanism have kicked in.
        // the lock remains locked; so the threads calling future.get remain blocked
        sleepMillis(callTimeout * 5);

        // unlocking the lock
        UnlockOperation op = new UnlockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), otherThreadId);
        opService.createInvocationBuilder(null, op, partitionId)
                .setCallTimeout(callTimeout)
                .invoke()
                .join();

        // now the futures should all unblock
        for (Future f : futures) {
            assertEquals(Boolean.TRUE, f.get());
        }
    }

    /**
     * Tests if the future on a blocking operation can be shared by multiple threads. This tests fails in 3.6 because
     * only 1 thread will be able to swap out CONTINUE_WAIT and all other threads will fail with an OperationTimeoutExcepyion
     */
    @Test
    public void async_whenMultipleAndThenOnSameFuture() {
        int callTimeout = 5000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        final HazelcastInstance remote = factory.newHazelcastInstance(config);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(local);

        String key = generateKeyOwnedBy(remote);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        // first we execute an operation that stall the partition.
        OperationServiceImpl opService = nodeEngine.getOperationService();

        // first we are going to lock
        int otherThreadId = 1;
        LockOperation otherOp = new LockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), otherThreadId, -1, -1);
        opService.createInvocationBuilder(null, otherOp, partitionId)
                .setCallTimeout(callTimeout)
                .invoke()
                .join();

        // then we are going to send another lock request by a different thread; so it can't complete
        int thisThreadId = 2;
        LockOperation thisOp = new LockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), thisThreadId, -1, -1);
        final InternalCompletableFuture<Object> future = opService.createInvocationBuilder(null, thisOp, partitionId)
                .setCallTimeout(callTimeout)
                .invoke();

        // then we register a bunch of listeners
        int listenerCount = 10;
        final CountDownLatch listenersCompleteLatch = new CountDownLatch(listenerCount);
        for (int k = 0; k < 10; k++) {
            future.whenCompleteAsync((response, t) -> {
                if (t == null) {
                    if (Boolean.TRUE.equals(response)) {
                        listenersCompleteLatch.countDown();
                    } else {
                        System.out.println(response);
                    }
                } else {
                    t.printStackTrace();
                }
            });
        }

        // let's do a very long wait so that the heartbeat/retrying mechanism have kicked in.
        // the lock remains locked; so the threads calling future.get remain blocked
        sleepMillis(callTimeout * 5);

        // unlocking the lock
        UnlockOperation op = new UnlockOperation(new DistributedObjectNamespace(SERVICE_NAME, key), nodeEngine.toData(key), otherThreadId);
        opService.createInvocationBuilder(null, op, partitionId)
                .setCallTimeout(callTimeout)
                .invoke()
                .join();

        // and all the listeners should complete
        assertOpenEventually(listenersCompleteLatch);
    }

    @Test
    public void sync_testInterruption() throws InterruptedException {
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
    public void sync_testWaitingIndefinitely() throws InterruptedException {
        final Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "6000");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);

        // need to warm-up partitions, since waiting for queue backup can take up to 5 seconds
        // and that may cause OperationTimeoutException with "No response for 4000 ms" error
        warmUpPartitions(instances);

        final String name = randomName();
        IQueue queue = instances[0].getQueue(name);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                // because max timeout=6000 we get timeout exception which we should not
                instances[1].getQueue(name).take();
                latch.countDown();
            } catch (Exception ignored) {
                ignored.printStackTrace();
            }
        }).start();

        // wait for enough time which is greater than max-timeout (6000)
        sleepSeconds(10);
        queue.offer("item");

        assertTrue(latch.await(20, SECONDS));
    }

    @Test
    public void sync_testWaitingWithTimeout() throws InterruptedException {
        final Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "6000");
        final HazelcastInstance hz = createHazelcastInstance(config);
        final CountDownLatch latch = new CountDownLatch(1);

        final IQueue queue = hz.getQueue(randomName());

        spawn(() -> {
            try {
                queue.poll(10, SECONDS);
                latch.countDown();
            } catch (Exception ignored) {
                ignored.printStackTrace();
            }
        });

        assertTrue("latch failed to open", latch.await(20, SECONDS));
    }

    @Test
    public void sync_whenInterruptionDuringBlockingOp() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        final IQueue queue = hz.getQueue(randomName());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean interruptedFlag = new AtomicBoolean(false);

        final OpThread thread = new OpThread("Queue-Poll-Thread", latch, interruptedFlag) {
            protected void doOp() throws InterruptedException {
                assertNotNull(queue.poll(1, TimeUnit.MINUTES));
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        queue.offer("item");

        assertTrue(latch.await(1, TimeUnit.MINUTES));

        if (thread.isInterruptionCaught()) {
            assertFalse("Thread interrupted flag should not be set!", interruptedFlag.get());
            assertFalse("Queue should not be empty!", queue.isEmpty());
        } else {
            assertTrue("Thread interrupted flag should be set! " + thread, interruptedFlag.get());
            assertTrue("Queue should be empty!", queue.isEmpty());
        }
    }

    @SuppressWarnings("unchecked")
    private static BiConsumer<Object, Throwable> getExecutionCallbackMock() {
        return mock(BiConsumer.class);
    }

    private abstract static class OpThread extends Thread {

        final CountDownLatch latch;
        final AtomicBoolean interruptionCaught = new AtomicBoolean(false);
        final AtomicBoolean interruptedFlag;

        OpThread(String name, CountDownLatch latch, AtomicBoolean interruptedFlag) {
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

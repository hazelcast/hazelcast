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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.operations.IsLockedOperation;
import com.hazelcast.concurrent.lock.operations.LockOperation;
import com.hazelcast.concurrent.lock.operations.UnlockOperation;
import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
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

import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
        InternalOperationService opService = nodeEngine.getOperationService();
        opService.invokeOnPartition(null, new SlowOperation(5 * callTimeout), partitionId);

        // then we execute a lock operation that won't be executed because the partition is blocked.
        LockOperation op = new LockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), 1, -1, -1);
        InternalCompletableFuture<Object> future = opService.createInvocationBuilder(null, op, partitionId)
                .setCallTimeout(callTimeout)
                .invoke();

        try {
            future.join();
            fail("Invocation should failed with timeout!");
        } catch (OperationTimeoutException expected) {
            ignore(expected);
        }

        IsLockedOperation isLockedOperation = new IsLockedOperation(new InternalLockNamespace(key), nodeEngine.toData(key), 1);
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
        InternalOperationService opService = nodeEngine.getOperationService();
        opService.invokeOnPartition(null, new SlowOperation(5 * callTimeout), partitionId);

        // then we execute a lock operation that won't be executed because the partition is blocked.
        LockOperation op = new LockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), 1, -1, -1);
        InternalCompletableFuture<Object> future = opService.createInvocationBuilder(null, op, partitionId)
                .setCallTimeout(callTimeout)
                .invoke();

        // then we register our callback
        final ExecutionCallback<Object> callback = getExecutionCallbackMock();
        future.andThen(callback);

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
        InternalLockNamespace namespace = new InternalLockNamespace(key);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        // first we lock the lock by another thread
        InternalOperationService opService = nodeEngine.getOperationService();
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
        InternalOperationService opService = nodeEngine.getOperationService();
        int otherThreadId = 2;
        opService.invokeOnPartition(
                new LockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), otherThreadId, -1, -1)
                        .setPartitionId(partitionId))
                .join();

        // then we execute a lock operation that won't be executed because lock is already acquired
        // we are going to do some waiting (3x call timeout)
        int threadId = 1;
        Operation op = new LockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), threadId, -1, 3 * callTimeout)
                .setPartitionId(partitionId);
        final InternalCompletableFuture<Object> future = opService.invokeOnPartition(op);

        final ExecutionCallback<Object> callback = getExecutionCallbackMock();
        future.andThen(callback);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback).onResponse(Boolean.FALSE);
            }
        });
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
        InternalOperationService opService = nodeEngine.getOperationService();
        opService.invokeOnPartition(null, new SlowOperation(SECONDS.toMillis(5)), partitionId);

        // then we execute a lock operation that won't be executed because the partition is blocked.
        LockOperation op = new LockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), 1, -1, -1);
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

    private void assertFailsEventuallyWithOperationTimeoutException(final ExecutionCallback callback) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                ArgumentCaptor<Throwable> argument = ArgumentCaptor.forClass(Throwable.class);
                verify(callback).onFailure(argument.capture());

                assertInstanceOf(OperationTimeoutException.class, argument.getValue());
            }
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
        InternalOperationService opService = nodeEngine.getOperationService();

        // first we are going to lock
        int otherThreadId = 1;
        opService.createInvocationBuilder
                (null, new LockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), otherThreadId, -1, -1), partitionId)
                .setCallTimeout(callTimeout)
                .invoke()
                .join();

        // then we are going to send the invocation and share the future by many threads
        int thisThreadId = 2;
        final InternalCompletableFuture<Object> future = opService.createInvocationBuilder
                (null, new LockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), thisThreadId, -1, -1), partitionId)
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
        opService.createInvocationBuilder
                (null, new UnlockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), otherThreadId), partitionId)
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
    public void async_whenMultipleAndThenOnSameFuture() throws Exception {
        int callTimeout = 5000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        final HazelcastInstance remote = factory.newHazelcastInstance(config);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(local);

        String key = generateKeyOwnedBy(remote);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        // first we execute an operation that stall the partition.
        InternalOperationService opService = nodeEngine.getOperationService();

        // first we are going to lock
        int otherThreadId = 1;
        opService.createInvocationBuilder
                (null, new LockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), otherThreadId, -1, -1), partitionId)
                .setCallTimeout(callTimeout)
                .invoke()
                .join();

        // then we are going to send another lock request by a different thread; so it can't complete
        int thisThreadId = 2;
        final InternalCompletableFuture<Object> future = opService.createInvocationBuilder
                (null, new LockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), thisThreadId, -1, -1), partitionId)
                .setCallTimeout(callTimeout)
                .invoke();

        // then we register a bunch of listeners
        int listenerCount = 10;
        final CountDownLatch listenersCompleteLatch = new CountDownLatch(listenerCount);
        for (int k = 0; k < 10; k++) {
            future.andThen(new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    if (Boolean.TRUE.equals(response)) {
                        listenersCompleteLatch.countDown();
                    } else {
                        System.out.println(response);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    t.printStackTrace();
                }
            });
        }

        // let's do a very long wait so that the heartbeat/retrying mechanism have kicked in.
        // the lock remains locked; so the threads calling future.get remain blocked
        sleepMillis(callTimeout * 5);

        // unlocking the lock
        opService.createInvocationBuilder
                (null, new UnlockOperation(new InternalLockNamespace(key), nodeEngine.toData(key), otherThreadId), partitionId)
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

        // need to warm-up partitions, since waiting for lock backup can take up to 5 seconds
        // and that may cause OperationTimeoutException with "No response for 4000 ms" error
        warmUpPartitions(instances);

        final String name = randomName();
        ILock lock = instances[0].getLock(name);
        lock.lock();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    // because max timeout=6000 we get timeout exception which we should not
                    instances[1].getLock(name).lock();
                    latch.countDown();
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
            }
        }.start();

        // wait for enough time which is greater than max-timeout (6000)
        sleepSeconds(10);
        lock.unlock();

        assertTrue(latch.await(20, SECONDS));
    }

    @Test
    public void sync_testWaitingInfinitelyForTryLock() throws InterruptedException {
        final Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "6000");
        final HazelcastInstance hz = createHazelcastInstance(config);
        final CountDownLatch latch = new CountDownLatch(1);

        final ILock lock = hz.getLock(randomName());
        lock.lock();

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.tryLock(10, SECONDS);
                    latch.countDown();
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
            }
        });

        assertTrue("latch failed to open", latch.await(20, SECONDS));
    }

    @Test
    public void sync_whenInterruptionDuringBlockingOp2() throws InterruptedException {
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

    @SuppressWarnings("unchecked")
    private static ExecutionCallback<Object> getExecutionCallbackMock() {
        return mock(ExecutionCallback.class);
    }

    private abstract class OpThread extends Thread {

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

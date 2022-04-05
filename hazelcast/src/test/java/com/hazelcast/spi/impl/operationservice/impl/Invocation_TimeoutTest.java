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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.test.Accessors.getOperationService;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class Invocation_TimeoutTest extends HazelcastTestSupport {

    private static final Object RESPONSE = "someresponse";

    /**
     * Tests if the get is called with a timeout, and the operation takes more time to execute then the timeout, that the call
     * fails with a TimeoutException.
     */
    @Test
    public void whenGetTimeout_thenTimeoutException() throws InterruptedException, ExecutionException {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);
        Future future = opService.invokeOnPartition(
                null,
                new SlowOperation(SECONDS.toMillis(10), RESPONSE),
                getPartitionId(remote));

        try {
            future.get(1, SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        // so even though the previous get failed with a timeout, the future can still provide a valid result.
        assertEquals(RESPONSE, future.get());
    }

    @Test
    public void whenMultipleThreadsCallGetOnSameLongRunningOperation() throws ExecutionException, InterruptedException {
        long callTimeout = 5000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);
        final Future future = opService.invokeOnPartition(
                null,
                new SlowOperation(callTimeout * 3, RESPONSE),
                getPartitionId(remote));

        List<Future> futures = new LinkedList<Future>();
        for (int k = 0; k < 10; k++) {
            futures.add(spawn(() -> future.get()));
        }

        for (Future sf : futures) {
            assertEquals(RESPONSE, sf.get());
        }
    }

    // ==================== long running operation ===============================================================================
    // Tests that a long running operation is not going to give any problems.
    //
    // When an operation is running for a long time, so a much longer time than the call timeout and heartbeat time, due to
    // the heartbeats being detected, the call will not timeout and returns a valid response.
    // ===========================================================================================================================

    @Test
    public void sync_whenLongRunningOperation() throws InterruptedException, ExecutionException, TimeoutException {
        long callTimeout = 10000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        Future future = opService.invokeOnPartition(
                null,
                new SlowOperation(6 * callTimeout, RESPONSE),
                getPartitionId(remote));

        Object result = future.get(120, SECONDS);
        assertEquals(RESPONSE, result);
    }

    @Test
    public void async_whenLongRunningOperation() {
        long callTimeout = 10000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);
        InternalCompletableFuture<Object> future = opService.invokeOnPartition(
                null,
                new SlowOperation(6 * callTimeout, RESPONSE),
                getPartitionId(remote));

        final BiConsumer<Object, Throwable> callback = getExecutionCallbackMock();
        future.whenCompleteAsync(callback);

        assertTrueEventually(() -> verify(callback).accept(RESPONSE, null));
    }

    // ==================== operation heartbeat timeout ==========================================================================
    // This test verifies that an Invocation is going to timeout when no heartbeat is received.
    //
    // This is simulated by executing an void operation (an operation that doesn't send a response). After the execution of this
    // operation, no heartbeats will be received since it has executed successfully. So eventually the heartbeat timeout should
    // kick in.
    // ===========================================================================================================================

    @Test
    public void sync_whenHeartbeatTimeout_thenOperationTimeoutException() throws Exception {
        long callTimeoutMs = 5000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        Future future = opService.invokeOnPartition(
                null,
                new VoidOperation(),
                getPartitionId(remote));

        try {
            future.get(5 * callTimeoutMs, MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertInstanceOf(OperationTimeoutException.class, cause);
            assertContains(cause.getMessage(), "operation-heartbeat-timeout");
        }
    }

    @Test
    public void async_whenHeartbeatTimeout_thenOperationTimeoutException() {
        long callTimeoutMs = 1000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        InternalCompletableFuture<Object> future = opService.invokeOnPartition(
                null,
                new VoidOperation(),
                getPartitionId(remote));

        BiConsumer<Object, Throwable> callback = getExecutionCallbackMock();
        future.whenCompleteAsync(callback);

        assertEventuallyFailsWithHeartbeatTimeout(callback);
    }

    // ==================== eventually operation heartbeat timeout ===============================================================
    // This test verifies that an Invocation is going to timeout when initially there was a heartbeat, but eventually this
    // heartbeat stops
    //
    // This is done by creating a void operation that runs for an extended period and on completion, the void operation doesn't
    // send a response.
    // ===========================================================================================================================

    @Test
    public void sync_whenEventuallyHeartbeatTimeout_thenOperationTimeoutException() throws Exception {
        long callTimeoutMs = 5000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        Future future = opService.invokeOnPartition(
                null,
                new VoidOperation(callTimeoutMs * 5),
                getPartitionId(remote));

        try {
            future.get(10 * callTimeoutMs, MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertInstanceOf(OperationTimeoutException.class, cause);
            assertContains(cause.getMessage(), "operation-heartbeat-timeout");
        }
    }

    @Test
    public void async_whenEventuallyHeartbeatTimeout_thenOperationTimeoutException() {
        long callTimeoutMs = 5000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        InternalCompletableFuture<Object> future = opService.invokeOnPartition(
                null,
                new VoidOperation(callTimeoutMs * 5),
                getPartitionId(remote));

        final BiConsumer<Object, Throwable> callback = getExecutionCallbackMock();
        future.whenCompleteAsync(callback);

        assertEventuallyFailsWithHeartbeatTimeout(callback);
    }

    // ==================== operation call timeout ===============================================================================
    // This test verifies that an operation doesn't get executed after its timeout expires. This is done by
    // executing an operation in front of the operation that takes a lot of time to execute.
    // ===========================================================================================================================

    @Test
    public void sync_whenCallTimeout_thenOperationTimeoutException() throws Exception {
        long callTimeoutMs = 60000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        int partitionId = getPartitionId(remote);
        long slowOperationDurationMs = (long) (callTimeoutMs * 1.1);
        opService.invokeOnPartition(new SlowOperation(slowOperationDurationMs).setPartitionId(partitionId));

        Future future = opService.invokeOnPartition(new DummyOperation().setPartitionId(partitionId));

        try {
            future.get(3 * callTimeoutMs, MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertInstanceOf(OperationTimeoutException.class, cause);
            assertContains(cause.getMessage(), "operation-call-timeout");
        }
    }

    @Test
    public void async_whenCallTimeout_thenOperationTimeoutException() {
        long callTimeoutMs = 60000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        int partitionId = getPartitionId(remote);
        long slowOperationDurationMs = (long) (callTimeoutMs * 1.1);
        opService.invokeOnPartition(new SlowOperation(slowOperationDurationMs).setPartitionId(partitionId));

        InternalCompletableFuture<Object> future = opService.invokeOnPartition(new DummyOperation().setPartitionId(partitionId));

        BiConsumer<Object, Throwable> callback = getExecutionCallbackMock();
        future.whenCompleteAsync(callback);

        assertEventuallyFailsWithCallTimeout(callback);
    }

    @SuppressWarnings("unchecked")
    private static BiConsumer<Object, Throwable> getExecutionCallbackMock() {
        return mock(BiConsumer.class);
    }

    private static void assertEventuallyFailsWithHeartbeatTimeout(final BiConsumer<Object, Throwable> callback) {
        assertTrueEventually(() -> {
            ArgumentCaptor<Throwable> argument = ArgumentCaptor.forClass(Throwable.class);
            verify(callback).accept(isNull(), argument.capture());
            Throwable cause = argument.getValue();
            assertInstanceOf(OperationTimeoutException.class, cause);
            assertContains(cause.getMessage(), "operation-heartbeat-timeout");
        });
    }

    private static void assertEventuallyFailsWithCallTimeout(final BiConsumer<Object, Throwable> callback) {
        assertTrueEventually(() -> {
            ArgumentCaptor<Throwable> argument = ArgumentCaptor.forClass(Throwable.class);
            verify(callback).accept(isNull(), argument.capture());

            Throwable cause = argument.getValue();
            assertInstanceOf(OperationTimeoutException.class, cause);
            assertContains(cause.getMessage(), "operation-call-timeout");
        });
    }
}

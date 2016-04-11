package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.OperationService;
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
import org.mockito.ArgumentCaptor;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Invocation_TimeoutTest extends HazelcastTestSupport {

    private final static Object RESPONSE = "someresponse";

    private void assertEventuallyFailsWithHeartbeatTimeout(final ExecutionCallback callback) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                ArgumentCaptor<Throwable> argument = ArgumentCaptor.forClass(Throwable.class);
                verify(callback).onFailure(argument.capture());
                Throwable cause = argument.getValue();
                assertInstanceOf(OperationTimeoutException.class, cause);
                assertTrue(cause.getMessage(), cause.getMessage().contains("operation-heartbeat-timeout"));
            }
        });
    }

    private void assertEventuallyFailsWithCallTimeout(final ExecutionCallback callback) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                ArgumentCaptor<Throwable> argument = ArgumentCaptor.forClass(Throwable.class);
                verify(callback).onFailure(argument.capture());

                Throwable cause = argument.getValue();
                assertInstanceOf(OperationTimeoutException.class, cause);
                assertTrue(cause.getMessage(), cause.getMessage().contains("operation-call-timeout"));
            }
        });
    }


    /**
     * Tests if the get is called with a timeout, and the operation takes more time to execute then the timeout, that the call
     * fails with a TimeoutException.
     */
    @Test
    public void whenGetTimeout_thenTimeoutException() throws InterruptedException, ExecutionException, TimeoutException {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);
        Future f = opService.invokeOnPartition(
                null,
                new SlowOperation(SECONDS.toMillis(10), RESPONSE),
                getPartitionId(remote));

        try {
            f.get(1, SECONDS);
            fail();
        } catch (TimeoutException expected) {
        }

        // so even though the previous get failed with a timeout, the future can still provide a valid result.
        assertEquals(RESPONSE, f.get());
    }

    @Test
    public void whenMultipleThreadsCallGetOnSameLongRunningOperation() throws ExecutionException, InterruptedException {
        long callTimeout = 5000;
        Config config = new Config().setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);
        final Future f = opService.invokeOnPartition(
                null,
                new SlowOperation(callTimeout * 3, RESPONSE),
                getPartitionId(remote));

        List<Future> futures = new LinkedList<Future>();
        for (int k = 0; k < 10; k++) {
            futures.add(spawn(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return f.get();
                }
            }));
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
        long callTimeout = 5000;
        Config config = new Config().setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        Future f = opService.invokeOnPartition(
                null,
                new SlowOperation(6 * callTimeout, RESPONSE),
                getPartitionId(remote));

        Object result = f.get(120, SECONDS);
        assertEquals(RESPONSE, result);
    }

    @Test
    public void async_whenLongRunningOperation() throws InterruptedException, ExecutionException, TimeoutException {
        long callTimeout = 5000;
        Config config = new Config().setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);
        ICompletableFuture f = opService.invokeOnPartition(
                null,
                new SlowOperation(6 * callTimeout, RESPONSE),
                getPartitionId(remote));

        final ExecutionCallback callback = mock(ExecutionCallback.class);
        f.andThen(callback);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback).onResponse(RESPONSE);
            }
        });
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
        Config config = new Config().setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        Future f = opService.invokeOnPartition(
                null,
                new VoidOperation(),
                getPartitionId(remote));

        try {
            f.get(5 * callTimeoutMs, MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertInstanceOf(OperationTimeoutException.class, cause);
            assertTrue(cause.getMessage(), cause.getMessage().contains("operation-heartbeat-timeout"));
        }
    }

    @Test
    public void async_whenHeartbeatTimeout_thenOperationTimeoutException() throws Exception {
        long callTimeoutMs = 1000;
        Config config = new Config().setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        ICompletableFuture f = opService.invokeOnPartition(
                null,
                new VoidOperation(),
                getPartitionId(remote));

        ExecutionCallback callback = mock(ExecutionCallback.class);
        f.andThen(callback);

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
        Config config = new Config().setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        Future f = opService.invokeOnPartition(
                null,
                new VoidOperation(callTimeoutMs * 5),
                getPartitionId(remote));

        try {
            f.get(10 * callTimeoutMs, MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertInstanceOf(OperationTimeoutException.class, cause);
            assertTrue(cause.getMessage(), cause.getMessage().contains("operation-heartbeat-timeout"));
        }
    }

    @Test
    public void async_whenEventuallyHeartbeatTimeout_thenOperationTimeoutException() throws Exception {
        long callTimeoutMs = 5000;
        Config config = new Config().setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        ICompletableFuture f = opService.invokeOnPartition(
                null,
                new VoidOperation(callTimeoutMs * 5),
                getPartitionId(remote));

        final ExecutionCallback callback = mock(ExecutionCallback.class);
        f.andThen(callback);

        assertEventuallyFailsWithHeartbeatTimeout(callback);
    }


    // ==================== operation call timeout ===============================================================================
    // This test verifies that an operation doesn't get executed after its timeout expires. This is done by
    // executing an operation in front of the operation that takes a lot of time to execute.
    // ===========================================================================================================================

    @Test
    public void sync_whenCallTimeout_thenOperationTimeoutException() throws Exception {
        long callTimeoutMs = 5000;
        Config config = new Config().setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        int partitionId = getPartitionId(remote);
        opService.invokeOnPartition(null, new SlowOperation(callTimeoutMs * 2), partitionId);

        Future f = opService.invokeOnPartition(null, new DummyOperation(), partitionId);

        try {
            f.get(3 * callTimeoutMs, MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertInstanceOf(OperationTimeoutException.class, cause);
            assertTrue(cause.getMessage(), cause.getMessage().contains("operation-call-timeout"));
        }
    }

    @Test
    public void async_whenCallTimeout_thenOperationTimeoutException() throws Exception {
        long callTimeoutMs = 5000;
        Config config = new Config().setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMs);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        int partitionId = getPartitionId(remote);
        opService.invokeOnPartition(null, new SlowOperation(callTimeoutMs * 2), partitionId);

        ICompletableFuture f = opService.invokeOnPartition(null, new DummyOperation(), partitionId);

        ExecutionCallback callback = mock(ExecutionCallback.class);
        f.andThen(callback);

        assertEventuallyFailsWithCallTimeout(callback);
    }
}

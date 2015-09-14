package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_AndThenTest extends HazelcastTestSupport {
    private HazelcastInstance local;
    private InternalOperationService operationService;

    @Before
    public void setup() {
        local = createHazelcastInstance();
        operationService = getOperationService(local);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNullCallback() {
        Operation op = new DummyPartitionOperation(null);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        future.andThen(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNullCallback2() {
        Operation op = new DummyPartitionOperation(null);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        future.andThen(null, mock(Executor.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNullExecutor() {
        Operation op = new DummyPartitionOperation(null);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        future.andThen(mock(ExecutionCallback.class), null);
    }

    @Test
    public void whenOperationCompletes_thenCallbackNotified() throws Exception {
        final String response = "foo";
        Operation op = new DummyPartitionOperation(response).setDelayMs(1000);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        final ExecutionCallback callback = mock(ExecutionCallback.class);
        Executor executor = newSingleThreadExecutor();

        future.andThen(callback, executor);

        assertEventuallyCallbackOnResponse(response, callback);
    }

    @Test
    public void whenMultipleCallbacksRegistered() throws Exception {
        final String response = "foo";
        Operation op = new DummyPartitionOperation(response).setDelayMs(1000);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        final ExecutionCallback callback1 = mock(ExecutionCallback.class);
        final ExecutionCallback callback2 = mock(ExecutionCallback.class);
        final ExecutionCallback callback3 = mock(ExecutionCallback.class);
        Executor executor = newSingleThreadExecutor();

        future.andThen(callback1, executor);
        future.andThen(callback2, executor);
        future.andThen(callback3, executor);

        assertEventuallyCallbackOnResponse(response, callback1);
        assertEventuallyCallbackOnResponse(response, callback1);
        assertEventuallyCallbackOnResponse(response, callback1);
    }

    private void assertEventuallyCallbackOnResponse(final String response, final ExecutionCallback callback) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback).onResponse(response);
            }
        });
    }

    public void whenCallbackOnResponseThrowsException() {

    }

    public void whenCallbackOnFailureThrowsException() {

    }

    @Test
    public void whenOperationCompletesWithError_thenCallbackNotified() throws Exception {
        Operation op = new DummyPartitionOperation() {
            @Override
            public void run() throws Exception {
                throw new ExpectedRuntimeException();
            }
        };

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        final ExecutionCallback callback = mock(ExecutionCallback.class);
        Executor executor = newSingleThreadExecutor();

        future.andThen(callback, executor);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback).onFailure(isA(ExpectedRuntimeException.class));
            }
        });
    }

    @Test
    public void whenRegisteredAfterCompletion_thenCallbackImmediatelyNotified() throws Exception {
        String response = "foo";
        Operation op = new DummyPartitionOperation(response);

        InvocationFuture future = (InvocationFuture) operationService.invokeOnTarget(null, op, getAddress(local));
        future.get();

        ExecutionCallback callback = mock(ExecutionCallback.class);
        Executor executor = new Executor() {
            @Override
            public void execute(Runnable command) {
                command.run();
            }
        };

        future.andThen(callback, executor);

        verify(callback).onResponse(response);
        assertNull(future.callbackHead);
    }

    // There is a bug: https://github.com/hazelcast/hazelcast/issues/5001
    @Test
    public void whenNullResponse_thenCallbackExecuted() throws ExecutionException, InterruptedException {
        Operation op = new DummyPartitionOperation(null);
        final ExecutionCallback callback = mock(ExecutionCallback.class);
        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));
        future.get();

        // Callback can be completed immediately since a response (NULL_RESPONSE) has been already set.
        future.andThen(callback);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback, times(1)).onResponse(isNull());
            }
        });
    }
}

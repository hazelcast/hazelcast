package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InternalCompletableFuture;
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
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.mockito.Matchers.eq;
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
        DummyOperation op = new DummyOperation(null);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        future.andThen(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNullCallback2() {
        DummyOperation op = new DummyOperation(null);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        future.andThen(null, mock(Executor.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNullExecutor() {
        DummyOperation op = new DummyOperation(null);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        future.andThen(mock(ExecutionCallback.class), null);
    }

    // There is a bug: https://github.com/hazelcast/hazelcast/issues/5001
    @Test
    public void whenNullResponse_thenCallbackExecuted() throws ExecutionException, InterruptedException {
        DummyOperation op = new DummyOperation(null);
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

    @Test
    public void whenNotNullResponse_thenCallbackExecuted() throws ExecutionException, InterruptedException {
        final Object response = "foo";
        DummyOperation op = new DummyOperation(response);
        final ExecutionCallback callback = mock(ExecutionCallback.class);
        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));
        future.get();

        // Callback can be completed immediately since a response (NULL_RESPONSE) has been already set.
        future.andThen(callback);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback, times(1)).onResponse(eq(response));
            }
        });
    }

    @Test
    public void whenExceptionResponse_thenCallbackExecuted() throws ExecutionException, InterruptedException {
        final Object response = new ExpectedRuntimeException();
        DummyOperation op = new DummyOperation(response);
        final ExecutionCallback callback = mock(ExecutionCallback.class);
        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));
        // Callback can be completed immediately since a response (NULL_RESPONSE) has been already set.
        future.andThen(callback);

        final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback, times(1)).onFailure(captor.capture());
            }
        });

        assertInstanceOf(ExpectedRuntimeException.class, captor.getValue());
    }
}

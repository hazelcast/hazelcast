package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicInvocationFutureTest extends HazelcastTestSupport {
    private HazelcastInstance local;
    private InternalOperationService operationService;

    @Before
    public void setup() {
        local = createHazelcastInstance();
        Node node = getNode(local);
        operationService = node.nodeEngine.operationService;
    }


    public static Address getAddress(HazelcastInstance hz) {
        Node node = getNode(hz);
        return node.clusterService.getThisAddress();
    }

    // ================= isDone ==========================================

    @Test
    public void isDone_whenNullResponse() throws ExecutionException, InterruptedException {
        DummyPartitionOperation op = new DummyPartitionOperation(null);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));
        // first we wait for the future to complete.
        future.get();

        assertTrue(future.isDone());
    }

    @Test
    public void isDone_whenWaitResponse() {
        DummyPartitionOperation op = new DummyPartitionOperation() {
            @Override
            public void run() throws Exception {
                Thread.sleep(5000);
            }
        };

        BasicInvocationFuture future = (BasicInvocationFuture) operationService.invokeOnTarget(null, op, getAddress(local));
        future.set(BasicInvocation.WAIT_RESPONSE);
        assertFalse(future.isDone());
    }

    @Test
    public void isDone_whenInterruptedResponse() {
        DummyPartitionOperation op = new DummyPartitionOperation() {
            @Override
            public void run() throws Exception {
                Thread.sleep(5000);
            }
        };

        BasicInvocationFuture future = (BasicInvocationFuture) operationService.invokeOnTarget(null, op, getAddress(local));
        future.set(BasicInvocation.INTERRUPTED_RESPONSE);
        assertTrue(future.isDone());
    }

    @Test
    public void isDone_whenTimeoutResponse() {
        DummyPartitionOperation op = new DummyPartitionOperation() {
            @Override
            public void run() throws Exception {
                Thread.sleep(5000);
            }
        };

        BasicInvocationFuture future = (BasicInvocationFuture) operationService.invokeOnTarget(null, op, getAddress(local));
        future.set(BasicInvocation.TIMEOUT_RESPONSE);
        assertTrue(future.isDone());
    }

    @Test
    public void isDone_whenNoResponse() {
        DummyPartitionOperation op = new DummyPartitionOperation() {
            @Override
            public void run() throws Exception {
                Thread.sleep(5000);
            }
        };

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));
        System.out.println("Calling future.isDone");
        assertFalse(future.isDone());
    }

    @Test
    public void isDone_whenObjectResponse() {
        DummyPartitionOperation op = new DummyPartitionOperation() {
            @Override
            public void run() throws Exception {
                Thread.sleep(5000);
            }
        };

        BasicInvocationFuture future = (BasicInvocationFuture) operationService.invokeOnTarget(null, op, getAddress(local));
        future.set("someobject");
        assertTrue(future.isDone());
    }

    // ========================= andThen ==================================

    // There is a bug: https://github.com/hazelcast/hazelcast/issues/5001
    @Test
    public void andThen_whenNullResponse_thenCallbackExecuted() throws ExecutionException, InterruptedException {
        DummyPartitionOperation op = new DummyPartitionOperation(null);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));
        // first we wait for the future to complete.
        future.get();

        // if we now register a callback, it could complete immediately since a response already has been set (NULL_RESPONSE)
        final ExecutionCallback callback = mock(ExecutionCallback.class);
        future.andThen(callback);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback, times(1)).onResponse(isNull());
            }
        });
    }
}

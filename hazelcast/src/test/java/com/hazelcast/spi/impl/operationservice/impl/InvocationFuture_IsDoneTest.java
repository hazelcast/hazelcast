package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_IsDoneTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private InternalOperationService operationService;

    @Before
    public void setup() {
        local = createHazelcastInstance();
        operationService = getOperationService(local);
    }

    @Test
    public void whenNullResponse() throws ExecutionException, InterruptedException {
        DummyOperation op = new DummyOperation(null);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));
        future.get();

        assertTrue(future.isDone());
    }

    @Test
    public void whenWaitResponse() {
        DummyOperation op = new GetLostPartitionOperation();

        InvocationFuture future = (InvocationFuture) operationService.invokeOnTarget(null, op, getAddress(local));
        future.set(InternalResponse.WAIT_RESPONSE);

        assertFalse(future.isDone());
    }

    @Test
    public void whenInterruptedResponse() {
        DummyOperation op = new GetLostPartitionOperation();

        InvocationFuture future = (InvocationFuture) operationService.invokeOnTarget(null, op, getAddress(local));
        future.set(InternalResponse.INTERRUPTED_RESPONSE);

        assertTrue(future.isDone());
    }

    @Test
    public void whenTimeoutResponse() {
        DummyOperation op = new GetLostPartitionOperation();

        InvocationFuture future = (InvocationFuture) operationService.invokeOnTarget(null, op, getAddress(local));
        future.set(InternalResponse.TIMEOUT_RESPONSE);

        assertTrue(future.isDone());
    }

    @Test
    public void isDone_whenNoResponse() {
        DummyOperation op = new GetLostPartitionOperation();

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        assertFalse(future.isDone());
    }

    @Test
    public void isDone_whenObjectResponse() {
        DummyOperation op = new DummyOperation("foobar");

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        assertTrue(future.isDone());
    }


    // Needed to have an invocation and this is the easiest way how to get one and do not bother with its result.
    private static class GetLostPartitionOperation extends DummyOperation {
        {
            // we need to set the call-id to prevent running the operation on the calling-thread.
            setPartitionId(1);
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(5000);
        }
    }

}

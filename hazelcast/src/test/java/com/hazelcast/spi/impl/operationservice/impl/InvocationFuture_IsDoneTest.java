package com.hazelcast.spi.impl.operationservice.impl;

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

import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.CALL_TIMEOUT_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.DEAD_OPERATION_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.INTERRUPTED_RESPONSE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_IsDoneTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private InternalOperationService opService;

    @Before
    public void setup() {
        local = createHazelcastInstance();
        opService = getOperationService(local);
    }

    private InvocationFuture invoke(Operation op) {
        if (op.getPartitionId() >= 0) {
            return (InvocationFuture) opService.invokeOnPartition(null, op, op.getPartitionId());
        } else {
            return (InvocationFuture) opService.invokeOnTarget(null, op, getAddress(local));
        }
    }

    @Test
    public void whenNullResponse() throws ExecutionException, InterruptedException {
        InternalCompletableFuture future = invoke(new DummyPartitionOperation(null));
        // first we wait for the future to complete.
        future.get();

        boolean done = future.isDone();

        assertTrue(done);
    }

    @Test
    public void whenInterruptedResponse() {
        InvocationFuture future = invoke(new DummyPartitionOperation().setDelayMs(5000));
        future.set(INTERRUPTED_RESPONSE);

        boolean done = future.isDone();

        assertTrue(done);
    }

    @Test
    public void whenTimeoutResponse() {
        InvocationFuture future = invoke(new DummyPartitionOperation().setDelayMs(5000));
        future.set(CALL_TIMEOUT_RESPONSE);

        boolean done = future.isDone();

        assertTrue(done);
    }

    @Test
    public void whenDeadOperationResponse() {
        InvocationFuture future = invoke(new DummyPartitionOperation().setDelayMs(5000));
        future.set(DEAD_OPERATION_RESPONSE);

        boolean done = future.isDone();

        assertTrue(done);
    }

    @Test
    public void whenNoResponse() {
        InternalCompletableFuture future = invoke(new DummyPartitionOperation().setDelayMs(5000));

        boolean done = future.isDone();

        assertFalse(done);
    }

    @Test
    public void whenObjectResponse() throws Exception {
        Operation op = new DummyOperation("foobar");

        InternalCompletableFuture future = invoke(op);

        // since it isn't a partition specific operation, it will be run on the thread that calls invokeOnTarget.
        assertTrue(future.isDone());
    }

    @Test
    public void whenOperationThrowsExceptionResponse() throws Exception {
        InternalCompletableFuture future = invoke(new DummyPartitionOperation() {
            public void run() {
                throw new ExpectedRuntimeException();
            }
        });
        // call the get to make sure the invocation completes
        try {
            future.get();
            fail();
        } catch (ExecutionException expected) {
        }

        boolean done = future.isDone();

        assertTrue(done);
    }

    @Test
    public void whenSomeWaitingNeeded() {
        Operation op = new DummyPartitionOperation().setDelayMs(1000);

        final InternalCompletableFuture future = opService.invokeOnTarget(null, op, getAddress(local));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(future.isDone());
            }
        });
    }
}

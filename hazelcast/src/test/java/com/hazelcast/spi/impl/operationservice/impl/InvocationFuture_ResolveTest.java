package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.DeadOperationException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.CALL_TIMEOUT_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.DEAD_OPERATION_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.INTERRUPTED_RESPONSE;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_ResolveTest extends HazelcastTestSupport {
    private HazelcastInstance hz;
    private InternalOperationService opService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        opService = getOperationService(hz);
    }

    @After
    @Before
    public void cleanup() {
        // since we are messing with the interrupted flag in this test, lets clear it
        Thread.interrupted();
    }

    private InvocationFuture invoke(Operation op) {
        return (InvocationFuture) opService.invokeOnPartition(null, op, op.getPartitionId());
    }

    @Test(expected = InterruptedException.class)
    public void whenInterruptedResponse() throws ExecutionException, InterruptedException {
        InvocationFuture f = invoke(new DummyPartitionOperation().setDelayMs(5000));
        f.resolve(INTERRUPTED_RESPONSE);
    }

    @Test
    public void whenDeadOperationResponse() throws ExecutionException, InterruptedException {
        InvocationFuture f = invoke(new DummyPartitionOperation().setDelayMs(5000));

        try {
            f.resolve(DEAD_OPERATION_RESPONSE);
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(DeadOperationException.class, e.getCause());
        }
    }

    @Test
    public void whenCallTimeoutResponse() throws ExecutionException, InterruptedException {
        InvocationFuture f = invoke(new DummyPartitionOperation().setDelayMs(5000));

        try {
            f.resolve(CALL_TIMEOUT_RESPONSE);
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(OperationTimeoutException.class, e.getCause());
        }
    }

    @Test
    public void whenNullResponse() throws Exception {
        InvocationFuture f = invoke(new DummyPartitionOperation().setDelayMs(5000));

        Object result = f.resolve(null);

        assertNull(result);
    }

    @Test
    public void whenNormalObjectResponse() throws Exception {
        InvocationFuture f = invoke(new DummyPartitionOperation().setDelayMs(5000));
        Object response = "foo";

        Object result = f.resolve(response);

        assertSame(response, result);
    }

    @Test
    public void whenExceptionResponse() {
    }

    @Test
    public void whenDataResponse() {
    }
}

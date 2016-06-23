package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_CancelTest extends HazelcastTestSupport {

    private static final int RESULT = 123;

    private InternalOperationService opService;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        opService = getOperationService(hz);
    }

    @Test
    public void whenMayInterruptIfRunning_thenIgnore() throws Exception {
        ICompletableFuture future = invoke();

        boolean result = future.cancel(false);

        assertFalse(result);
        assertFalse(future.isCancelled());
        assertFalse(future.isDone());
        // we need to make sure that the future is still running
        assertEquals(RESULT, future.get());
    }

    @Test
    public void whenMayNotInterruptIfRunning_thenIgnore() throws Exception {
        ICompletableFuture future = invoke();

        boolean result = future.cancel(true);

        assertFalse(result);
        assertFalse(future.isCancelled());
        assertFalse(future.isDone());
        assertEquals(RESULT, future.get());
    }

    private InternalCompletableFuture invoke() {
        Operation op = new Operation() {
            @Override
            public void run() throws Exception {
                sleepMillis(1000);
            }

            @Override
            public Object getResponse() {
                return RESULT;
            }
        };
        return opService.invokeOnPartition(null, op, 0);
    }
}

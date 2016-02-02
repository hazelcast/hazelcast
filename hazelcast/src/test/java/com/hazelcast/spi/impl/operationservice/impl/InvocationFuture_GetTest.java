package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InternalCompletableFuture;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_GetTest extends HazelcastTestSupport {

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

    private InternalCompletableFuture invoke(Operation op) {
        return opService.invokeOnPartition(null, op, op.getPartitionId());
    }

    @Test
    public void whenSomeWaitingNeeded() throws Exception {
        InternalCompletableFuture f = invoke(new DummyPartitionOperation(1).setDelayMs(1000));

        Object result = f.get();

        assertEquals(1, result);
    }

    @Test
    public void whenNoWaitingNeeded() throws Exception {
        InternalCompletableFuture f = invoke(new DummyPartitionOperation(1).setDelayMs(1000));

        // first we wait till the future completes
        f.get();

        Object result = f.get();

        assertEquals(1, result);
    }

    @Test
    public void whenInterruptedDuringWaitOnNonInterruptableOperation_thenInterruptRestored() throws Exception {
        Thread.currentThread().interrupt();

        InvocationFuture f = (InvocationFuture) invoke(new DummyPartitionOperation(1));

        Object result = f.get();

        assertEquals(1, result);
        // we need to make sure that the interrupt flag has been restored.
        assertTrue(Thread.currentThread().isInterrupted());
        // since we are not executing a blocking operation, the interrupted flag should not be raided on the invocation
        assertFalse(f.invocation.interrupted);
    }
}

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InternalCompletableFuture;
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

import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_GetWithTimeoutTest extends HazelcastTestSupport {

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

    @Test(expected = NullPointerException.class)
    public void whenNullTimeout() throws Exception {
        InternalCompletableFuture f = invoke(new DummyOperation(null));

        f.get(0, null);
    }

    private InvocationFuture invoke(DummyOperation op) {
        return (InvocationFuture) opService.invokeOnPartition(null, op, 0);
    }

    @Test
    public void whenNegativeTimeoutButValueAvailable_thenValueReturned() throws Exception {
        InvocationFuture f = invoke(new DummyPartitionOperation(1));
        //first we wait for the result
        f.get();

        //now we wait a second time for the result but with a timeut.
        Object result = f.get(-1, MINUTES);

        assertEquals(1, result);
    }

    @Test
    public void whenSomeWaitingNeeded() throws Exception {
        InvocationFuture f = invoke(new DummyPartitionOperation(1).setDelayMs(1000));

        Object result = f.get(1, MINUTES);

        assertEquals(1, result);
    }

    @Test
    public void whenTooMuchWaiting_thenTimeoutException() throws Exception {
        InvocationFuture f = invoke(new DummyPartitionOperation(1).setDelayMs(1000));

        try {
            f.get(1, MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
        }

        // even though the get ran into a timeout, the future will still be able to complete when there
        // is sufficient timeout. So a thread running into an TimeoutException doesn't screw up the future.
        assertEquals(1, f.get());
    }

    @Test
    public void whenInterruptedDuringWaitOnNonInterruptableOperation_thenInterruptRestored() throws Exception {
        Thread.currentThread().interrupt();

        InvocationFuture f = invoke(new DummyPartitionOperation(1));

        Object result = f.get(1, MINUTES);

        assertEquals(1, result);
        // we need to make sure that the interrupt flag has been restored.
        assertTrue(Thread.currentThread().isInterrupted());
        // since we are not executing a blocking operation, the interrupted flag should not be raised on the invocation
        assertFalse(f.invocation.interrupted);
    }
}

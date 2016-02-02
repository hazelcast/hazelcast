package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
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
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_GetSafelyTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private InternalOperationService opService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        opService = getOperationService(hz);
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void whenOperationThrowsExpectedRuntimeException() throws ExecutionException, InterruptedException {
        InternalCompletableFuture f = invoke(new AbstractOperation() {
            @Override
            public void run() throws Exception {
                throw new ExpectedRuntimeException();
            }
        });

        f.getSafely();
    }

    @Test
    public void whenOperationThrowsTimeoutException() throws ExecutionException, InterruptedException {
        InternalCompletableFuture f = invoke(new AbstractOperation() {
            @Override
            public void run() throws Exception {
                throw new TimeoutException();
            }
        });

        try {
            f.getSafely();
            fail();
        } catch (HazelcastException e) {
            assertInstanceOf(TimeoutException.class, e.getCause());
        }
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void whenOperationThrowsExecutionException() throws ExecutionException, InterruptedException {
        InternalCompletableFuture f = invoke(new AbstractOperation() {
            @Override
            public void run() throws Exception {
                throw new ExecutionException("", new ExpectedRuntimeException());
            }
        });

        f.getSafely();
    }

    @Test
    public void whenOperationThrowsExpectedCheckedException() throws ExecutionException, InterruptedException {
        InternalCompletableFuture f = invoke(new AbstractOperation() {
            @Override
            public void run() throws Exception {
                throw new ExpectedCheckedException();
            }
        });

        try {
            f.getSafely();
            fail();
        } catch (HazelcastException e) {
            assertInstanceOf(ExpectedCheckedException.class, e.getCause());
        }
    }

    private InternalCompletableFuture invoke(AbstractOperation operation) {
        return opService.invokeOnPartition(null, operation, 0);
    }

    static class ExpectedCheckedException extends Exception {
    }
}

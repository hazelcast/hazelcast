package com.hazelcast.spi.impl.operationservice.impl;

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
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_ExceptionHandlingTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private InternalOperationService opService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        opService = getOperationService(hz);
    }

    @Test
    public void whenOperationThrowsExpectedRuntimeException() throws ExecutionException, InterruptedException {
        Future f = invoke(new DummyPartitionOperation() {
            @Override
            public void run() throws Exception {
                throw new ExpectedRuntimeException();
            }
        });

        try {
            f.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(ExpectedRuntimeException.class, e.getCause());
        }
    }

    @Test
    public void whenOperationThrowsTimeoutException() throws ExecutionException, InterruptedException {
        Future f = invoke(new DummyPartitionOperation() {
            @Override
            public void run() throws Exception {
                throw new TimeoutException();
            }
        });

        try {
            f.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(TimeoutException.class, e.getCause());
        }
    }

    @Test
    public void whenOperationThrowsExecutionException() throws ExecutionException, InterruptedException {
        Future f = invoke(new DummyPartitionOperation() {
            @Override
            public void run() throws Exception {
                throw new ExecutionException("", new ExpectedRuntimeException());
            }
        });

        try {
            f.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(ExecutionException.class, e.getCause());
            assertInstanceOf(ExpectedRuntimeException.class, e.getCause().getCause());
        }
    }

    @Test
    public void whenOperationThrowsExpectedCheckedException() throws ExecutionException, InterruptedException {
        Future f = invoke(new DummyPartitionOperation() {
            @Override
            public void run() throws Exception {
                throw new ExpectedCheckedException();
            }
        });

        try {
            f.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(ExpectedCheckedException.class, e.getCause());
        }
    }

    private InternalCompletableFuture invoke(AbstractOperation operation) {
        return opService.invokeOnPartition(null, operation, 0);
    }

    static class ExpectedCheckedException extends Exception {
    }
}

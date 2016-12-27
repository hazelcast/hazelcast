package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractInvocationFuture_ClosedExecutorTest extends AbstractInvocationFuture_AbstractTest {


    @Test
    public void whenCompleteBeforeShutdown_thenCallback() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        TestFuture future = new TestFuture(executorService, logger);
        future.complete(new Object());
        executorService.shutdown();
        final AtomicBoolean onFailure = new AtomicBoolean();
        future.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {

            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof RejectedExecutionException) {
                    onFailure.set(true);
                }
            }
        });
        assertTrue(onFailure.get());
    }

    @Test
    public void whenCompleteAfterShutdown_thenCallback() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        TestFuture future = new TestFuture(executorService, logger);
        executorService.shutdown();
        future.complete(new Object());
        final AtomicBoolean onFailure = new AtomicBoolean();
        future.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {

            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof RejectedExecutionException) {
                    onFailure.set(true);
                }
            }
        });
        assertTrue(onFailure.get());
    }

}

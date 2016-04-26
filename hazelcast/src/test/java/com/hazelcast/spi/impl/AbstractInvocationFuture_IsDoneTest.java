package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractInvocationFuture_IsDoneTest extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void whenVoid() {
        assertFalse(future.isDone());
    }

    @Test
    public void whenNullResult() {
        future.complete(null);
        assertTrue(future.isDone());
    }

    @Test
    public void whenNoneNullResult() {
        future.complete(value);
        assertTrue(future.isDone());
    }

    @Test
    public void whenExceptionalResult() {
        future.complete(new RuntimeException());
        assertTrue(future.isDone());
    }

    @Test
    public void whenBlockingThread() {
        spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.get();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotSame(AbstractInvocationFuture.VOID, future.getState());
            }
        });

        assertFalse(future.isDone());
    }

    @Test
    public void whenCallbackWithoutCustomExecutor() {
        future.andThen(mock(ExecutionCallback.class));

        assertFalse(future.isDone());
    }

    @Test
    public void whenCallbackWithCustomExecutor() {
        future.andThen(mock(ExecutionCallback.class), mock(Executor.class));

        assertFalse(future.isDone());
    }

    @Test
    public void whenMultipleWaiters() {
        future.andThen(mock(ExecutionCallback.class), mock(Executor.class));
        future.andThen(mock(ExecutionCallback.class), mock(Executor.class));

        assertFalse(future.isDone());
    }

}

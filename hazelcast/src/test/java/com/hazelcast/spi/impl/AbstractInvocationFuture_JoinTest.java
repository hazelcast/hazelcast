package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.spi.impl.AbstractInvocationFuture.VOID;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The Join forwards to {@link AbstractInvocationFuture#get()}. So most of the testing will be
 * in the {@link AbstractInvocationFuture_GetTest}. This test contains mostly the exception handling.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractInvocationFuture_JoinTest extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void whenNormalResponse() throws ExecutionException, InterruptedException {
        future.complete(value);

        Future joinFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.join();
            }
        });

        assertCompletesEventually(joinFuture);
        assertSame(value, joinFuture.get());
    }

    @Test
    public void whenRuntimeException() throws Exception {
        ExpectedRuntimeException ex = new ExpectedRuntimeException();
        future.complete(ex);

        Future joinFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.join();
            }
        });

        assertCompletesEventually(joinFuture);
        try {
            joinFuture.get();
            fail();
        } catch (ExecutionException e) {
            assertSame(ex, e.getCause());
        }
    }

    @Test
    public void whenRegularException() throws Exception {
        Exception ex = new Exception();
        future.complete(ex);

        Future joinFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.join();
            }
        });

        assertCompletesEventually(joinFuture);
        try {
            joinFuture.get();
            fail();
        } catch (ExecutionException e) {
            // The 'ex' is wrapped in an unchecked HazelcastException
            HazelcastException hzEx = assertInstanceOf(HazelcastException.class, e.getCause());
            assertSame(ex, hzEx.getCause());
        }
    }

    @Test
    public void whenInterrupted() throws Exception {
        final AtomicReference<Thread> thread = new AtomicReference<Thread>();
        final AtomicBoolean interrupted = new AtomicBoolean();
        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                thread.set(Thread.currentThread());
                try {
                    return future.join();
                } finally {
                    interrupted.set(Thread.currentThread().isInterrupted());
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotSame(VOID, future.getState());
            }
        });

        sleepSeconds(5);
        thread.get().interrupt();

        assertCompletesEventually(getFuture);
        assertTrue(interrupted.get());

        try {
            future.join();
            fail();
        } catch (HazelcastException e) {
            assertInstanceOf(InterruptedException.class, e.getCause());
        }
    }
}

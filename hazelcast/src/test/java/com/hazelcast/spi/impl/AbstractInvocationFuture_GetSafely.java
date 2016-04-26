package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastException;
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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractInvocationFuture_GetSafely extends AbstractInvocationFuture_AbstractTest{

    @Test
    public void whenNormalResponse() throws ExecutionException, InterruptedException {
        future.complete(value);

        Future joinFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.getSafely();
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
                return future.getSafely();
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
                return future.getSafely();
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
}

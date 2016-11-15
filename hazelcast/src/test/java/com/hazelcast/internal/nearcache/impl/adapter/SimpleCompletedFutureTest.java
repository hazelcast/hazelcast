package com.hazelcast.internal.nearcache.impl.adapter;

import com.hazelcast.core.ExecutionCallback;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleCompletedFutureTest {

    private SimpleCompletedFuture<String> future;

    @Before
    public void setUp() {
        future = new SimpleCompletedFuture<String>("test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAndThen() {
        future.andThen(new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
            }

            @Override
            public void onFailure(Throwable t) {
            }
        });
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAndThen_withExecutor() {
        future.andThen(new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
            }

            @Override
            public void onFailure(Throwable t) {
            }
        }, null);
    }

    @Test
    public void testCancel() {
        assertFalse(future.cancel(false));
        assertFalse(future.isCancelled());

        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());
    }

    @Test
    public void testIsDone() {
        assertTrue(future.isDone());
    }

    @Test
    public void testGet() throws Exception {
        assertEquals("test", future.get());
    }

    @Test
    public void testGet_withTimeout() throws Exception {
        assertEquals("test", future.get(5, TimeUnit.SECONDS));
    }
}

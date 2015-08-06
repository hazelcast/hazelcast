package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.spi.impl.AbstractCompletableFuture.ExecutionCallbackNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractCompletableFutureTest extends HazelcastTestSupport {

    private final static Object FAKE_GET_RESPONSE = "foobar";

    private HazelcastInstance hz;
    private ILogger logger;
    private NodeEngineImpl nodeEngine;
    private Executor executor;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        nodeEngine = getNodeEngineImpl(hz);
        logger = Logger.getLogger(AbstractCompletableFutureTest.class);
        executor = Executors.newFixedThreadPool(1);
    }

    // ==================== setResult ===========================================

    @Test
    public void setResult_whenPendingCallback() {
        setResult_whenPendingCallback(null);
        setResult_whenPendingCallback("foo");
        setResult_whenPendingCallback(new Exception());
    }

    public void setResult_whenPendingCallback(final Object result) {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        final ExecutionCallback callback1 = mock(ExecutionCallback.class);
        final ExecutionCallback callback2 = mock(ExecutionCallback.class);
        future.andThen(callback1);
        future.andThen(callback2);

        future.setResult(result);

        assertSame(result, future.state);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                if (result instanceof Throwable) {
                    verify(callback1).onFailure((Throwable) result);
                } else {
                    verify(callback1).onResponse(result);
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                if (result instanceof Throwable) {
                    verify(callback2).onFailure((Throwable) result);
                } else {
                    verify(callback2).onResponse(result);
                }
            }
        });
    }

    @Test
    public void setResult_whenNoPendingCallback() {
        setResult_whenNoPendingCallback(null);
        setResult_whenNoPendingCallback("foo");
    }

    public void setResult_whenNoPendingCallback(Object result) {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        future.setResult(result);

        assertSame(result, future.state);
    }


    @Test
    public void setResult_whenResultAlreadySet() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        Object initialResult = "firstresult";

        future.setResult(initialResult);

        future.setResult("secondresult");
        assertSame(initialResult, future.state);
    }

    // ==================== get ===========================================

    @Test
    public void get() throws ExecutionException, InterruptedException {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        Object result = future.get();

        assertSame(FAKE_GET_RESPONSE, result);
        assertEquals(Long.MAX_VALUE, future.timeout);
        assertEquals(TimeUnit.MILLISECONDS, future.unit);
    }

    @Test
    public void get_whenTimeout() throws ExecutionException, InterruptedException {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.getException = new TimeoutException();
        Object result = future.get();

        assertNull(result);
    }

    // ==================== getResult ===========================================

    @Test
    public void getResult_whenInitialState() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        Object result = future.getResult();

        assertNull(result);
    }

    @Test
    public void getResult_whenPendingCallback() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.andThen(mock(ExecutionCallback.class));

        Object result = future.getResult();

        assertNull(result);
    }

    @Test
    public void getResult_whenNullResult() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(null);

        Object result = future.getResult();

        assertNull(result);
    }

    @Test
    public void getResult_whenNoneNullResult() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        Object expectedResult = "result";
        future.setResult(expectedResult);

        Object result = future.getResult();

        assertSame(expectedResult, result);
    }

    // ==================== andThen ===========================================

    @Test(expected = IllegalArgumentException.class)
    public void andThen_whenNullCallback() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.andThen(null, executor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void andThen_whenNullExecutor() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.andThen(mock(ExecutionCallback.class), null);
    }

    @Test
    public void andThen_whenResultAvailable() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        final Object result = "result";
        future.setResult(result);

        final ExecutionCallback callback = mock(ExecutionCallback.class);
        future.andThen(callback, executor);

        assertSame(result, future.state);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback).onResponse(result);
            }
        });
    }

    @Test
    public void andThen_whenInitialState() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        ExecutionCallback callback = mock(ExecutionCallback.class);
        future.andThen(callback, executor);

        ExecutionCallbackNode node = assertInstanceOf(ExecutionCallbackNode.class, future.state);
        assertSame(node.callback, callback);
        assertSame(node.executor, executor);
        assertSame(node.next, AbstractCompletableFuture.INITIAL_STATE);
        verifyZeroInteractions(callback);
    }

    @Test
    public void andThen_whenPendingCallback() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        ExecutionCallback callback1 = mock(ExecutionCallback.class);
        future.andThen(callback1, executor);

        ExecutionCallback callback2 = mock(ExecutionCallback.class);
        future.andThen(callback2, executor);

        ExecutionCallbackNode secondNode = assertInstanceOf(ExecutionCallbackNode.class, future.state);
        assertSame(secondNode.callback, callback2);
        assertSame(secondNode.executor, executor);

        ExecutionCallbackNode firstNode = secondNode.next;
        assertSame(firstNode.callback, callback1);
        assertSame(firstNode.executor, executor);
        assertSame(firstNode.next, AbstractCompletableFuture.INITIAL_STATE);

        verifyZeroInteractions(callback1);
        verifyZeroInteractions(callback2);
    }

    // ==================== isDone ===========================================

    @Test
    public void isDone_whenInitialState() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        boolean result = future.isDone();

        assertFalse(result);
    }

    @Test
    public void isDone_whenCallbackRegistered() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.andThen(mock(ExecutionCallback.class));

        boolean result = future.isDone();

        assertFalse(result);
    }

    @Test
    public void isDone_whenNullResultSet() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(null);

        boolean result = future.isDone();

        assertTrue(result);
    }

    @Test
    public void isDone_whenNoneNullResultSet() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult("foo");

        boolean result = future.isDone();

        assertTrue(result);
    }

    class FutureImpl extends AbstractCompletableFuture {

        private long timeout;
        private TimeUnit unit;
        private Exception getException;

        protected FutureImpl(NodeEngine nodeEngine, ILogger logger) {
            super(nodeEngine, logger);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            this.timeout = timeout;
            this.unit = unit;
            if(getException!=null){
                ExceptionUtil.sneakyThrow(getException);
            }
            return FAKE_GET_RESPONSE;
        }
    }
}

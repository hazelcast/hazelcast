package com.hazelcast.spi;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.operationservice.impl.DummyOperation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationBuilderImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationBuilderTest extends HazelcastTestSupport {

    private HazelcastInstance node;

    @Before
    public void setup() {
        node = createHazelcastInstance();
    }


    /**
     * If nothing set to callback-executor, default executor should be used.
     */
    @Test
    public void test_setCallbackExecutor_whenDefault() throws Exception {
        InvocationBuilder builder = new InvocationBuilderImpl(getNodeEngineImpl(node),
                MapService.SERVICE_NAME, new DummyOperation("value"), 0);

        final AtomicBoolean executed = new AtomicBoolean(false);

        builder.setExecutionCallback(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                executed.set(true);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        });

        Future<Object> future = builder.invoke();
        future.get();


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue("Callback should be executed in default executor", executed.get());
            }
        });
    }


    /**
     * When callback-executor is null, default executor should be used.
     */
    @Test
    public void test_setCallbackExecutor_whenNullPassed() throws Exception {
        InvocationBuilder builder = new InvocationBuilderImpl(getNodeEngineImpl(node),
                MapService.SERVICE_NAME, new DummyOperation("value"), 0);

        final AtomicBoolean executed = new AtomicBoolean(false);

        builder.setExecutionCallback(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                executed.set(true);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        }).setCallbackExecutor(null);

        Future<Object> future = builder.invoke();
        future.get();


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue("Callback should be executed in default executor", executed.get());
            }
        });
    }

    /**
     * When an executor set to callback-executor, callback should be executed in it.
     */
    @Test
    public void test_setCallbackExecutor() throws Exception {
        InvocationBuilder builder = new InvocationBuilderImpl(getNodeEngineImpl(node),
                MapService.SERVICE_NAME, new DummyOperation("value"), 0);

        AtomicBoolean executed = new AtomicBoolean(false);
        final DirectExecutor executor = new DirectExecutor(executed);

        final AtomicBoolean callbackExecuted = new AtomicBoolean(false);
        builder.setExecutionCallback(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                callbackExecuted.set(true);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        }).setCallbackExecutor(executor);

        Future<Object> future = builder.invoke();
        future.get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue("Supplied executor should be called", executor.isCalled());
                assertTrue("Callback should be executed in supplied executor", callbackExecuted.get());
            }
        });

    }

    /**
     * Executes supplied runnable in the caller thread.
     */
    private static class DirectExecutor implements Executor {

        private final AtomicBoolean called;

        public DirectExecutor(AtomicBoolean called) {
            this.called = called;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
            called.set(true);
        }

        boolean isCalled() {
            return called.get();
        }
    }
}
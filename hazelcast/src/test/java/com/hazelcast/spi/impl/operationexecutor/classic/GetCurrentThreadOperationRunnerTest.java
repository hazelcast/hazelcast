package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.test.AssertTask;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GetCurrentThreadOperationRunnerTest extends AbstractClassicOperationExecutorTest {

    @Test
    public void test_whenCallerIsNormalThread() {
        initExecutor();

        OperationRunner operationRunner = executor.getCurrentThreadOperationRunner();

        DummyOperationRunnerFactory f = (DummyOperationRunnerFactory) handlerFactory;

        assertSame(f.adhocHandler, operationRunner);
    }

    @Test
    public void test_whenCallerIsPartitionOperationThread() {
        initExecutor();

        final GetCurrentThreadOperationHandlerOperation op = new GetCurrentThreadOperationHandlerOperation();
        op.setPartitionId(0);

        executor.execute(op);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                OperationRunner expected = executor.getPartitionOperationRunners()[0];
                OperationRunner actual = op.getResponse();
                assertSame(expected, actual);
            }
        });
    }

    @Test
    public void test_whenCallerIsGenericOperationThread() {
        initExecutor();

        final GetCurrentThreadOperationHandlerOperation op = new GetCurrentThreadOperationHandlerOperation();
        op.setPartitionId(-1);

        executor.execute(op);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                boolean found = false;
                OperationRunner foundHandler = op.getResponse();
                for (OperationRunner h : executor.getGenericOperationRunners()) {
                    if (foundHandler == h) {
                        found = true;
                        break;
                    }
                }

                assertTrue("handler is not found is one of the generic handlers", found);
            }
        });
    }

    public class GetCurrentThreadOperationHandlerOperation extends AbstractOperation {
        volatile OperationRunner operationRunner;

        @Override
        public void run() throws Exception {
            operationRunner = executor.getCurrentThreadOperationRunner();
        }

        @Override
        public OperationRunner getResponse() {
            return operationRunner;
        }
    }
}

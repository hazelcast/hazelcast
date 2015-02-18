package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.OperationHandler;
import com.hazelcast.test.AssertTask;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GetCurrentThreadOperationHandlerTest extends AbstractClassicSchedulerTest {

    @Test
    public void test_whenCallerIsNormalThread() {
        initScheduler();

        OperationHandler operationHandler = scheduler.getCurrentThreadOperationHandler();

        DummyOperationHandlerFactory f = (DummyOperationHandlerFactory) handlerFactory;

        assertSame(f.adhocHandler, operationHandler);
    }

    @Test
    public void test_whenCallerIsPartitionOperationThread() {
        initScheduler();

        final GetCurrentThreadOperationHandlerOperation op = new GetCurrentThreadOperationHandlerOperation();
        op.setPartitionId(0);

        scheduler.execute(op);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                OperationHandler expected = scheduler.getPartitionOperationHandlers()[0];
                OperationHandler actual = op.getResponse();
                assertSame(expected, actual);
            }
        });
    }

    @Test
    public void test_whenCallerIsGenericOperationThread() {
        initScheduler();

        final GetCurrentThreadOperationHandlerOperation op = new GetCurrentThreadOperationHandlerOperation();
        op.setPartitionId(-1);

        scheduler.execute(op);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                boolean found = false;
                OperationHandler foundHandler = op.getResponse();
                for (OperationHandler h : scheduler.getGenericOperationHandlers()) {
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
        volatile OperationHandler operationHandler;

        @Override
        public void run() throws Exception {
            operationHandler = scheduler.getCurrentThreadOperationHandler();
        }

        @Override
        public OperationHandler getResponse() {
            return operationHandler;
        }
    }
}

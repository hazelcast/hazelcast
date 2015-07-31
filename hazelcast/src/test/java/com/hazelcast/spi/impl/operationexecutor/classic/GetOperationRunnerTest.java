package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.test.AssertTask;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GetOperationRunnerTest extends AbstractClassicOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNull() {
        initExecutor();

        executor.getOperationRunner(null);
    }

    @Test
    public void test_whenCallerIsNormalThread_andGenericOperation_thenReturnAdHocRunner() {
        initExecutor();

        Operation op = new DummyOperation(-1);
        OperationRunner operationRunner = executor.getOperationRunner(op);

        DummyOperationRunnerFactory f = (DummyOperationRunnerFactory) handlerFactory;
        assertSame(f.adhocHandler, operationRunner);
    }

    @Test
    public void test_whenPartitionSpecificOperation_thenReturnCorrectPartitionOperationRunner() {
        initExecutor();

        int partitionId = 0;

        Operation op = new DummyOperation(partitionId);
        OperationRunner runner = executor.getOperationRunner(op);

        assertSame(executor.getPartitionOperationRunners()[partitionId], runner);
    }

    @Test
    public void test_whenCallerIsGenericOperationThread() {
        initExecutor();

        Operation nestedOp = new DummyOperation(-1);


        final GetCurrentThreadOperationHandlerOperation op = new GetCurrentThreadOperationHandlerOperation(nestedOp);
        op.setPartitionId(Operation.GENERIC_PARTITION_ID);

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
        final Operation op;

        public GetCurrentThreadOperationHandlerOperation(Operation op) {
            this.op = op;
        }

        @Override
        public void run() throws Exception {
            operationRunner = executor.getOperationRunner(op);
        }

        @Override
        public OperationRunner getResponse() {
            return operationRunner;
        }
    }

    class DummyOperation extends AbstractOperation {
        DummyOperation(int partitionId) {
            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {

        }
    }
}

package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IsInvocationAllowedFromCurrentThreadTest extends AbstractClassicOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNullOperation() {
        initExecutor();

        executor.isInvocationAllowedFromCurrentThread(null, false);
    }

    // ============= generic operations ==============================

    @Test
    public void test_whenGenericOperation_andCallingFromUserThread() {
        initExecutor();

        DummyGenericOperation genericOperation = new DummyGenericOperation();

        boolean result = executor.isInvocationAllowedFromCurrentThread(genericOperation, false);

        assertTrue(result);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromPartitionOperationThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(0) {
            @Override
            public Object call() {
                return executor.isInvocationAllowedFromCurrentThread(genericOperation, false);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromGenericOperationThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(Operation.GENERIC_PARTITION_ID) {
            @Override
            public Object call() {
                return executor.isInvocationAllowedFromCurrentThread(genericOperation, false);
            }
        };
        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromNioThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isInvocationAllowedFromCurrentThread(genericOperation, false);
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        assertEqualsEventually(futureTask, Boolean.FALSE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromNioThread_andAsync() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isInvocationAllowedFromCurrentThread(genericOperation, true);
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        assertEqualsEventually(futureTask, Boolean.FALSE);
    }

    // ===================== partition specific operations ========================

    @Test
    public void test_whenPartitionOperation_andCallingFromUserThread() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        boolean result = executor.isInvocationAllowedFromCurrentThread(partitionOperation, false);

        assertTrue(result);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromGenericOperationThread() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(Operation.GENERIC_PARTITION_ID) {
            @Override
            public Object call() {
                return executor.isInvocationAllowedFromCurrentThread(partitionOperation, false);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andCorrectPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionOperation.getPartitionId()) {
            @Override
            public Object call() {
                return executor.isInvocationAllowedFromCurrentThread(partitionOperation, false);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andWrongPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        int wrongPartition = partitionOperation.getPartitionId()+1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartition) {
            @Override
            public Object call() {
                return executor.isInvocationAllowedFromCurrentThread(partitionOperation, false);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andWrongPartition_andAsync() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        int wrongPartition = partitionOperation.getPartitionId()+1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartition) {
            @Override
            public Object call() {
                return executor.isInvocationAllowedFromCurrentThread(partitionOperation, true);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromNioThread() {
        initExecutor();

        final Operation operation = new DummyOperation(1);

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isInvocationAllowedFromCurrentThread(operation, false);
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        assertEqualsEventually(futureTask, Boolean.FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromNioThread_andAsync() {
        initExecutor();

        final Operation operation = new DummyOperation(1);

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isInvocationAllowedFromCurrentThread(operation, true);
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        assertEqualsEventually(futureTask, Boolean.FALSE);
    }
}

package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IsAllowedToRunInCurrentThreadTest extends AbstractClassicOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNullOperation() {
        initExecutor();

        executor.isAllowedToRunInCurrentThread(null);
    }

    // ============= generic operations ==============================

    @Test
    public void test_whenGenericOperation_andCallingFromUserThread() {
        initExecutor();

        DummyGenericOperation genericOperation = new DummyGenericOperation();

        boolean result = executor.isAllowedToRunInCurrentThread(genericOperation);

        assertTrue(result);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromPartitionOperationThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable() {
            @Override
            public Object call() {
                return executor.isAllowedToRunInCurrentThread(genericOperation);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromGenericOperationThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(0) {
            @Override
            public Object call() {
                return executor.isAllowedToRunInCurrentThread(genericOperation);
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
                return executor.isAllowedToRunInCurrentThread(genericOperation);
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

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        boolean result = executor.isAllowedToRunInCurrentThread(partitionOperation);

        assertFalse(result);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromGenericThread() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        PartitionSpecificCallable task = new PartitionSpecificCallable(-1) {
            @Override
            public Object call() {
                return executor.isAllowedToRunInCurrentThread(partitionOperation);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andCorrectPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionOperation.getPartitionId()) {
            @Override
            public Object call() {
                return executor.isAllowedToRunInCurrentThread(partitionOperation);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andWrongPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        int wrongPartition = partitionOperation.getPartitionId() + 1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartition) {
            @Override
            public Object call() {
                return executor.isAllowedToRunInCurrentThread(partitionOperation);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromNioThread() {
        initExecutor();

        final DummyPartitionOperation operation = new DummyPartitionOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isAllowedToRunInCurrentThread(operation);
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        assertEqualsEventually(futureTask, Boolean.FALSE);
    }
}

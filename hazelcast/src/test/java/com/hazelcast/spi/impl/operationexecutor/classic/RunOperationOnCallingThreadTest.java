package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.AssertTask;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertTrue;

public class RunOperationOnCallingThreadTest extends AbstractClassicOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNull() {
        initExecutor();

        executor.runOnCallingThread(null);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromNormalThread() {
        initExecutor();

        DummyGenericOperation genericOperation = new DummyGenericOperation();

        executor.runOnCallingThread(genericOperation);

        DummyOperationRunner adhocHandler = ((DummyOperationRunnerFactory) handlerFactory).adhocHandler;
        assertTrue(adhocHandler.operations.contains(genericOperation));
    }

    @Test
    public void test_whenGenericOperation_andCallingFromGenericThread() {
        config.setProperty(GroupProperties.PROP_GENERIC_OPERATION_THREAD_COUNT, "1");
        initExecutor();

        final DummyOperationRunner genericOperationHandler = ((DummyOperationRunnerFactory) handlerFactory).genericOperationHandlers.get(0);
        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(-1) {
            @Override
            public Object call() {
                executor.runOnCallingThread(genericOperation);
                return null;
            }
        };

        executor.execute(task);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                boolean contains = genericOperationHandler.operations.contains(genericOperation);
                assertTrue("operation is not found in the generic operation handler", contains);
            }
        });
    }

    @Test
    public void test_whenGenericOperation_andCallingFromPartitionThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        final int partitionId = 0;
        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionId) {
            @Override
            public Object call() {
                executor.runOnCallingThread(genericOperation);
                return null;
            }
        };

        executor.execute(task);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyOperationRunner handler = (DummyOperationRunner) executor.getPartitionOperationRunners()[partitionId];
                assertTrue(handler.operations.contains(genericOperation));
            }
        });
    }

    // IO thread is now allowed to run any operation, so we expect an IllegalThreadStateException
    @Test
    public void test_whenGenericOperation_andCallingFromIOThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    executor.runOnCallingThread(genericOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return Boolean.TRUE;
                }
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        assertEqualsEventually(futureTask, Boolean.TRUE);
    }

    @Test(expected = IllegalThreadStateException.class)
    public void test_whenPartitionOperation_andCallingFromNormalThread() {
        initExecutor();

        DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        executor.runOnCallingThread(partitionOperation);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromGenericThread() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(-1) {
            @Override
            public Object call() {
                try {
                    executor.runOnCallingThread(partitionOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return Boolean.TRUE;
                }
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionThread_andWrongPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        int wrongPartitionId = partitionOperation.getPartitionId() + 1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartitionId) {
            @Override
            public Object call() {
                try {
                    executor.runOnCallingThread(partitionOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return Boolean.TRUE;
                }
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionThread_andRightPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        final int partitionId = partitionOperation.getPartitionId();
        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionId) {
            @Override
            public Object call() {
                executor.runOnCallingThread(partitionOperation);
                return Boolean.TRUE;
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyOperationRunner handler = (DummyOperationRunner) executor.getPartitionOperationRunners()[partitionId];
                assertTrue(handler.operations.contains(partitionOperation));
            }
        });
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromIOThread() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    executor.runOnCallingThread(partitionOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return Boolean.TRUE;
                }
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        assertEqualsEventually(futureTask, Boolean.TRUE);
    }
}

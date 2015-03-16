package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionSpecificCallable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProgressiveOperationExecutor_runOnCallingThreadTest extends AbstractProgressiveOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void whenNull_thenNullPointerException() {
        initExecutor();

        executor.runOnCallingThread(null);
    }

    @Test
    public void whenGenericOperation_andCallingFromNormalThread() {
        initExecutor();

        GenericOperation genericOperation = new GenericOperation();

        executor.runOnCallingThread(genericOperation);

        DummyOperationRunner adhocHandler = ((DummyOperationRunnerFactory) runnerFactory).adHocRunner;
        assertTrue(adhocHandler.operations.contains(genericOperation));
    }

    @Test
    public void whenGenericOperation_andCallingFromGenericThread() {
        config.setProperty(GroupProperties.PROP_GENERIC_OPERATION_THREAD_COUNT, "1");
        initExecutor();

        final DummyOperationRunner genericOperationHandler = ((DummyOperationRunnerFactory) runnerFactory).genericRunners.get(0);
        final GenericOperation genericOperation = new GenericOperation();

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
    public void whenGenericOperation_andCallingFromPartitionThread() {
        initExecutor();

        final GenericOperation genericOperation = new GenericOperation();

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

    // IO thread is not allowed to run any operation, so we expect an IllegalThreadStateException
    @Test
    public void whenGenericOperation_andCallingFromIOThread() {
        initExecutor();

        final GenericOperation genericOperation = new GenericOperation();

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
    public void whenPartitionOperation_andCallingFromNormalThread() {
        initExecutor();

        PartitionOperation partitionOperation = new PartitionOperation();

        executor.runOnCallingThread(partitionOperation);
    }

    @Test
    @Ignore
    public void whenPartitionOperation_andCallingFromGenericThread() {
        initExecutor();

        final PartitionOperation partitionOperation = new PartitionOperation();

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

        assertCompletesEventually(task, Boolean.TRUE);
    }

    @Test
    public void whenPartitionOperation_andCallingFromPartitionThread_andWrongPartition() {
        initExecutor();

        final PartitionOperation partitionOperation = new PartitionOperation();

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

        assertCompletesEventually(task, Boolean.TRUE);
    }

    @Test
    public void whenPartitionOperation_andCallingFromPartitionStealingThread_andRightPartition() throws Throwable {
        initExecutor();

        int partitionId = 0;
        final PartitionOperation innerPartitionOperation = new PartitionOperation(partitionId);
        PartitionOperation outerPartitionOperation = new PartitionOperation(partitionId) {
            @Override
            public Object call() throws Throwable {
                executor.runOnCallingThread(innerPartitionOperation);
                return null;
            }
        };
        outerPartitionOperation.setPartitionId(partitionId);

        executor.runOnCallingThreadIfPossible(outerPartitionOperation);

        awaitCompletion(innerPartitionOperation, outerPartitionOperation);
        assertExecutedByCurrentThread(outerPartitionOperation);
        assertExecutedByCurrentThread(innerPartitionOperation);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionStealingThread_andWrongPartition() throws Throwable {
        initExecutor();

        int partitionId = 0;
        final PartitionOperation innerPartitionOperation = new PartitionOperation(partitionId);

        PartitionOperation outerPartitionOperation = new PartitionOperation(innerPartitionOperation.getPartitionId() + 1) {
            @Override
            public Object call() throws Throwable {
                try {
                    executor.runOnCallingThread(innerPartitionOperation);
                    fail();
                } catch (IllegalThreadStateException expected) {
                }
                return null;
            }
        };

        outerPartitionOperation.setPartitionId(partitionId + 1);

        executor.runOnCallingThreadIfPossible(outerPartitionOperation);

        awaitCompletion(outerPartitionOperation);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionThread_andRightPartition() {
        initExecutor();

        final PartitionOperation partitionOperation = new PartitionOperation();

        final int partitionId = partitionOperation.getPartitionId();
        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionId) {
            @Override
            public Object call() {
                executor.runOnCallingThread(partitionOperation);
                return Boolean.TRUE;
            }
        };

        executor.execute(task);

        assertCompletesEventually(task, Boolean.TRUE);
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

        final PartitionOperation partitionOperation = new PartitionOperation();

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

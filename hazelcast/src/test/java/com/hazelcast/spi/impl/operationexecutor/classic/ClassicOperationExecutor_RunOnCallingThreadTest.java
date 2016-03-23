package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.internal.properties.GroupProperty;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static com.hazelcast.spi.Operation.GENERIC_PARTITION_ID;
import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClassicOperationExecutor_RunOnCallingThreadTest extends ClassicOperationExecutor_AbstractTest {

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
        config.setProperty(GroupProperty.GENERIC_OPERATION_THREAD_COUNT.getName(), "1");
        initExecutor();

        final DummyOperationRunner genericOperationHandler =
                ((DummyOperationRunnerFactory) handlerFactory).genericOperationHandlers.get(0);
        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(GENERIC_PARTITION_ID) {
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
                    return false;
                } catch (IllegalThreadStateException e) {
                    return true;
                }
            }
        });

        DummyOperationHostileThread thread = new DummyOperationHostileThread(futureTask);
        thread.start();

        assertEqualsEventually(futureTask, TRUE);
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

        PartitionSpecificCallable task = new PartitionSpecificCallable(GENERIC_PARTITION_ID) {
            @Override
            public Object call() {
                try {
                    executor.runOnCallingThread(partitionOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return TRUE;
                }
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
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
                    return TRUE;
                }
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
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
                return TRUE;
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
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
                    return false;
                } catch (IllegalThreadStateException e) {
                    return true;
                }
            }
        });

        DummyOperationHostileThread thread = new DummyOperationHostileThread(futureTask);
        thread.start();

        assertEqualsEventually(futureTask, TRUE);
    }
}

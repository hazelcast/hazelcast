package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.spi.impl.OperationHandler;
import com.hazelcast.test.AssertTask;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertTrue;

public class RunOperationOnCallingThreadTest extends AbstractClassicSchedulerTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNull() {
        initScheduler();

        scheduler.runOperationOnCallingThread(null);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromNormalThread() {
        initScheduler();

        DummyGenericOperation genericOperation = new DummyGenericOperation();

        scheduler.runOperationOnCallingThread(genericOperation);

        DummyOperationHandler adhocHandler = ((DummyOperationHandlerFactory) handlerFactory).adhocHandler;
        assertTrue(adhocHandler.operations.contains(genericOperation));
    }

    @Test
    public void test_whenGenericOperation_andCallingFromGenericThread() {
        config.setProperty(GroupProperties.PROP_GENERIC_OPERATION_THREAD_COUNT, "1");
        initScheduler();

        final DummyOperationHandler genericOperationHandler = ((DummyOperationHandlerFactory) handlerFactory).genericOperationHandlers.get(0);
        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(-1) {
            @Override
            public Object call() {
                scheduler.runOperationOnCallingThread(genericOperation);
                return null;
            }
        };

        scheduler.execute(task);

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
        initScheduler();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        final int partitionId = 0;
        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionId) {
            @Override
            public Object call() {
                scheduler.runOperationOnCallingThread(genericOperation);
                return null;
            }
        };

        scheduler.execute(task);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyOperationHandler handler = (DummyOperationHandler) scheduler.getPartitionOperationHandlers()[partitionId];
                assertTrue(handler.operations.contains(genericOperation));
            }
        });
    }

    // IO thread is now allowed to run any operation, so we expect an IllegalThreadStateException
    @Test
    public void test_whenGenericOperation_andCallingFromIOThread() {
        initScheduler();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    scheduler.runOperationOnCallingThread(genericOperation);
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
        initScheduler();

        DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        scheduler.runOperationOnCallingThread(partitionOperation);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromGenericThread() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(-1) {
            @Override
            public Object call() {
                try {
                    scheduler.runOperationOnCallingThread(partitionOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return Boolean.TRUE;
                }
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionThread_andWrongPartition() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        int wrongPartitionId = partitionOperation.getPartitionId() + 1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartitionId) {
            @Override
            public Object call() {
                try {
                    scheduler.runOperationOnCallingThread(partitionOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return Boolean.TRUE;
                }
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionThread_andRightPartition() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        final int partitionId = partitionOperation.getPartitionId();
        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionId) {
            @Override
            public Object call() {
                scheduler.runOperationOnCallingThread(partitionOperation);
                return Boolean.TRUE;
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyOperationHandler handler = (DummyOperationHandler)scheduler.getPartitionOperationHandlers()[partitionId];
                assertTrue(handler.operations.contains(partitionOperation));
            }
        });
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromIOThread() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    scheduler.runOperationOnCallingThread(partitionOperation);
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

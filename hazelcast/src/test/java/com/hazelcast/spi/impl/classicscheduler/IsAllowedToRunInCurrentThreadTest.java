package com.hazelcast.spi.impl.classicscheduler;

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
public class IsAllowedToRunInCurrentThreadTest extends AbstractClassicSchedulerTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNullOperation() {
        initScheduler();

        scheduler.isAllowedToRunInCurrentThread(null);
    }

    // ============= generic operations ==============================

    @Test
    public void test_whenGenericOperation_andCallingFromUserThread() {
        initScheduler();

        DummyGenericOperation genericOperation = new DummyGenericOperation();

        boolean result = scheduler.isAllowedToRunInCurrentThread(genericOperation);

        assertTrue(result);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromPartitionOperationThread() {
        initScheduler();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable() {
            @Override
            public Object call() {
                return scheduler.isAllowedToRunInCurrentThread(genericOperation);
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromGenericOperationThread() {
        initScheduler();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(0) {
            @Override
            public Object call() {
                return scheduler.isAllowedToRunInCurrentThread(genericOperation);
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromNioThread() {
        initScheduler();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return scheduler.isAllowedToRunInCurrentThread(genericOperation);
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        assertEqualsEventually(futureTask, Boolean.FALSE);
    }


    // ===================== partition specific operations ========================

    @Test
    public void test_whenPartitionOperation_andCallingFromUserThread() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        boolean result = scheduler.isAllowedToRunInCurrentThread(partitionOperation);

        assertFalse(result);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromGenericThread() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        PartitionSpecificCallable task = new PartitionSpecificCallable(-1) {
            @Override
            public Object call() {
                return scheduler.isAllowedToRunInCurrentThread(partitionOperation);
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andCorrectPartition() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionOperation.getPartitionId()) {
            @Override
            public Object call() {
                return scheduler.isAllowedToRunInCurrentThread(partitionOperation);
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andWrongPartition() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        int wrongPartition = partitionOperation.getPartitionId() + 1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartition) {
            @Override
            public Object call() {
                return scheduler.isAllowedToRunInCurrentThread(partitionOperation);
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromNioThread() {
        initScheduler();

        final DummyPartitionOperation operation = new DummyPartitionOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return scheduler.isAllowedToRunInCurrentThread(operation);
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        assertEqualsEventually(futureTask, Boolean.FALSE);
    }
}

package com.hazelcast.spi.impl.classicscheduler;

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
public class IsInvocationAllowedFromCurrentThreadTest extends AbstractClassicSchedulerTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNullOperation() {
        initScheduler();

        scheduler.isInvocationAllowedFromCurrentThread(null);
    }

    // ============= generic operations ==============================

    @Test
    public void test_whenGenericOperation_andCallingFromUserThread() {
        initScheduler();

        DummyGenericOperation genericOperation = new DummyGenericOperation();

        boolean result = scheduler.isInvocationAllowedFromCurrentThread(genericOperation);

        assertTrue(result);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromPartitionOperationThread() {
        initScheduler();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(0) {
            @Override
            public Object call() {
                return scheduler.isInvocationAllowedFromCurrentThread(genericOperation);
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromGenericOperationThread() {
        initScheduler();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(-1) {
            @Override
            public Object call() {
                return scheduler.isInvocationAllowedFromCurrentThread(genericOperation);
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
                return scheduler.isInvocationAllowedFromCurrentThread(genericOperation);
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

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        boolean result = scheduler.isInvocationAllowedFromCurrentThread(partitionOperation);

        assertTrue(result);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromGenericOperationThread() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(-1) {
            @Override
            public Object call() {
                return scheduler.isInvocationAllowedFromCurrentThread(partitionOperation);
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andCorrectPartition() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionOperation.getPartitionId()) {
            @Override
            public Object call() {
                return scheduler.isInvocationAllowedFromCurrentThread(partitionOperation);
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andWrongPartition() {
        initScheduler();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        int wrongPartition = partitionOperation.getPartitionId()+1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartition) {
            @Override
            public Object call() {
                return scheduler.isInvocationAllowedFromCurrentThread(partitionOperation);
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromNioThread() {
        initScheduler();

        final Operation operation = new DummyOperation(1);

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return scheduler.isInvocationAllowedFromCurrentThread(operation);
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        assertEqualsEventually(futureTask, Boolean.FALSE);
    }
}

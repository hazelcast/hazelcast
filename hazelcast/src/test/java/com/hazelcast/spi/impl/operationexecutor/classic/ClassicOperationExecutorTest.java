package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.GroupProperty;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClassicOperationExecutorTest extends AbstractClassicOperationExecutorTest {

    @Test
    public void testConstruction() {
        initExecutor();

        assertEquals(groupProperties.getInteger(GroupProperty.PARTITION_COUNT), executor.getPartitionOperationRunners().length);
        assertEquals(executor.getGenericOperationThreadCount(), executor.getGenericOperationRunners().length);

        assertEquals(groupProperties.getInteger(GroupProperty.PARTITION_OPERATION_THREAD_COUNT),
                executor.getPartitionOperationThreadCount());

        assertEquals(groupProperties.getInteger(GroupProperty.GENERIC_OPERATION_THREAD_COUNT),
                executor.getGenericOperationThreadCount());
    }

    @Test
    public void test_getRunningOperationCount() {
        initExecutor();

        executor.execute(new DummyOperation(Operation.GENERIC_PARTITION_ID).durationMs(2000));
        executor.execute(new DummyOperation(Operation.GENERIC_PARTITION_ID).durationMs(2000));

        executor.execute(new DummyOperation(0).durationMs(2000));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int runningOperationCount = executor.getRunningOperationCount();
                System.out.println("runningOperationCount:" + runningOperationCount);
                assertEquals(3, runningOperationCount);
            }
        });
    }

    @Test
    public void test_getOperationExecutorQueueSize() {
        initExecutor();

        // first we need to set the threads to work so the queues are going to be left alone.
        for (int k = 0; k < executor.getGenericOperationThreadCount(); k++) {
            executor.execute(new DummyOperation(Operation.GENERIC_PARTITION_ID).durationMs(2000));
        }
        for (int k = 0; k < executor.getPartitionOperationThreadCount(); k++) {
            executor.execute(new DummyOperation(k).durationMs(2000));
        }

        // now we throw in some work in the queues that wont' be picked up
        int count = 0;
        for (int l = 0; l < 3; l++) {
            for (int k = 0; k < executor.getGenericOperationThreadCount(); k++) {
                executor.execute(new DummyOperation(Operation.GENERIC_PARTITION_ID).durationMs(2000));
                count++;
            }
        }
        for (int l = 0; l < 5; l++) {
            for (int k = 0; k < executor.getPartitionOperationThreadCount(); k++) {
                executor.execute(new DummyOperation(k).durationMs(2000));
                count++;
            }
        }

        final int expectedCount = count;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expectedCount, executor.getOperationExecutorQueueSize());
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void test_runOnAllPartitionThreads_whenTaskNull() {
        initExecutor();
        executor.runOnAllPartitionThreads(null);
    }

    @Test
    public void test_runOnAllPartitionThreads() throws Exception {
        initExecutor();

        int threadCount = executor.getPartitionOperationThreadCount();
        final CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);

        executor.runOnAllPartitionThreads(new Runnable() {
            @Override
            public void run() {
                // current thread must be a PartitionOperationThread
                if (Thread.currentThread() instanceof PartitionOperationThread) {
                    try {
                        awaitBarrier(barrier);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        awaitBarrier(barrier);
    }

    @Test
    public void test_interruptAllPartitionThreads() throws Exception {
        initExecutor();

        int threadCount = executor.getPartitionOperationThreadCount();
        final CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);

        executor.runOnAllPartitionThreads(new Runnable() {
            @Override
            public void run() {
                // current thread must be a PartitionOperationThread
                if (Thread.currentThread() instanceof PartitionOperationThread) {
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException ignored) {
                    } finally {
                        try {
                            awaitBarrier(barrier);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
        });

        executor.interruptAllPartitionThreads();
        awaitBarrier(barrier);
    }

    private static void awaitBarrier(CyclicBarrier barrier) throws Exception {
        barrier.await(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
    }
}

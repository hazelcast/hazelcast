package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.properties.GroupProperty.GENERIC_OPERATION_THREAD_COUNT;
import static com.hazelcast.internal.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.internal.properties.GroupProperty.PARTITION_OPERATION_THREAD_COUNT;
import static com.hazelcast.internal.properties.GroupProperty.PRIORITY_GENERIC_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.Operation.GENERIC_PARTITION_ID;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClassicOperationExecutorTest extends AbstractClassicOperationExecutorTest {

    @Test
    public void testConstruction() {
        initExecutor();

        assertEquals(groupProperties.getInteger(PARTITION_COUNT), executor.getPartitionOperationRunners().length);
        assertEquals(executor.getGenericOperationThreadCount(), executor.getGenericOperationRunners().length);

        assertEquals(groupProperties.getInteger(PARTITION_OPERATION_THREAD_COUNT),
                executor.getPartitionOperationThreadCount());

        assertEquals(groupProperties.getInteger(GENERIC_OPERATION_THREAD_COUNT)
                        + groupProperties.getInteger(PRIORITY_GENERIC_OPERATION_THREAD_COUNT),
                executor.getGenericOperationThreadCount());
    }

    @Test
    public void test_getRunningOperationCount() {
        initExecutor();

        executor.execute(new DummyOperation(GENERIC_PARTITION_ID).durationMs(2000));
        executor.execute(new DummyOperation(GENERIC_PARTITION_ID).durationMs(2000));

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
    public void test_getOperationExecutorQueueSize_genericWork() {
        initExecutor();


        int priorityGenericCount = groupProperties.getInteger(PRIORITY_GENERIC_OPERATION_THREAD_COUNT);

        for (int k = 0; k < executor.getGenericOperationThreadCount() - priorityGenericCount; k++) {
            executor.execute(new DummyOperation(GENERIC_PARTITION_ID).durationMs(2000));
        }

        // now we throw in some work in the queues that wont' be picked up
        int count = 0;
        for (int l = 0; l < 3; l++) {
            for (int k = 0; k < executor.getGenericOperationThreadCount(); k++) {
                executor.execute(new DummyOperation(GENERIC_PARTITION_ID).durationMs(2000));
                count++;
            }
        }

        final int expectedCount = count;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                System.out.println(executor.getOperationExecutorQueueSize() + " expected:" + expectedCount);
                assertEquals(expectedCount, executor.getOperationExecutorQueueSize());
            }
        });
    }

    @Test
    public void test_getOperationExecutorQueueSize_partitionSpecificWork() {
        initExecutor();

        for (int k = 0; k < executor.getPartitionOperationThreadCount(); k++) {
            executor.execute(new DummyOperation(k).durationMs(2000));
        }

        int count = 0;
        // now we throw in some work in the queues that wont' be picked up
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

    @Test
    public void genericPriorityTaskIsPickedUpEvenWhenAllGenericThreadsBusy() {
        initExecutor();

        // lets keep the regular generic threads busy
        for (int k = 0; k < executor.getGenericOperationThreadCount() * 10; k++) {
            executor.execute(new DummyOperation(GENERIC_PARTITION_ID).durationMs(20000000));
        }

        final CountDownLatch open = new CountDownLatch(1);
        // then we schedule a priority task
        executor.execute(new UrgentDummyOperation(GENERIC_PARTITION_ID) {
            public void run() {
                open.countDown();
            }
        });

        // and verify it completes.
        assertOpenEventually(open);
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

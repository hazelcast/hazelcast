package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClassicOperationSchedulerTest extends AbstractClassicSchedulerTest {

    @Test
    public void testConstruction() {
        initScheduler();

        assertEquals(groupProperties.PARTITION_COUNT.getInteger(), scheduler.getPartitionOperationHandlers().length);
        assertEquals(scheduler.getGenericOperationThreadCount(), scheduler.getGenericOperationHandlers().length);

        assertEquals(groupProperties.PARTITION_OPERATION_THREAD_COUNT.getInteger(),
                scheduler.getPartitionOperationThreadCount());

        assertEquals(groupProperties.GENERIC_OPERATION_THREAD_COUNT.getInteger(),
                scheduler.getGenericOperationThreadCount());
    }

    @Test
    public void test_getRunningOperationCount() {
        initScheduler();

        scheduler.execute(new DummyOperation(-1).durationMs(2000));
        scheduler.execute(new DummyOperation(-1).durationMs(2000));

        scheduler.execute(new DummyOperation(0).durationMs(2000));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int runningOperationCount = scheduler.getRunningOperationCount();
                System.out.println("runningOperationCount:" + runningOperationCount);
                assertEquals(3, runningOperationCount);
            }
        });
    }

    @Test
    public void test_getOperationExecutorQueueSize() {
        initScheduler();

        // first we need to set the threads to work so the queues are going to be left alone.
        for (int k = 0; k < scheduler.getGenericOperationThreadCount(); k++) {
            scheduler.execute(new DummyOperation(-1).durationMs(2000));
        }
        for (int k = 0; k < scheduler.getPartitionOperationThreadCount(); k++) {
            scheduler.execute(new DummyOperation(k).durationMs(2000));
        }

        // now we throw in some work in the queues that wont' be picked up
        int count = 0;
        for (int l = 0; l < 3; l++) {
            for (int k = 0; k < scheduler.getGenericOperationThreadCount(); k++) {
                scheduler.execute(new DummyOperation(-1).durationMs(2000));
                count++;
            }
        }
        for (int l = 0; l < 5; l++) {
            for (int k = 0; k < scheduler.getPartitionOperationThreadCount(); k++) {
                scheduler.execute(new DummyOperation(k).durationMs(2000));
                count++;
            }
        }

        final int expectedCount = count;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expectedCount, scheduler.getOperationExecutorQueueSize());
            }
        });
    }

    @Test
    public void test_dumpPerformanceMetrics() {
        initScheduler();

        StringBuffer sb = new StringBuffer();
        scheduler.dumpPerformanceMetrics(sb);
        String content = sb.toString();


        assertTrue(content.contains("processedCount="));
    }
}

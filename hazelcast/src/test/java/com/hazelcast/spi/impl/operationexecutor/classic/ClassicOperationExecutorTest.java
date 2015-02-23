package com.hazelcast.spi.impl.operationexecutor.classic;

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
public class ClassicOperationExecutorTest extends AbstractClassicOperationExecutorTest {

    @Test
    public void testConstruction() {
        initExecutor();

        assertEquals(groupProperties.PARTITION_COUNT.getInteger(), executor.getPartitionOperationRunners().length);
        assertEquals(executor.getGenericOperationThreadCount(), executor.getGenericOperationRunners().length);

        assertEquals(groupProperties.PARTITION_OPERATION_THREAD_COUNT.getInteger(),
                executor.getPartitionOperationThreadCount());

        assertEquals(groupProperties.GENERIC_OPERATION_THREAD_COUNT.getInteger(),
                executor.getGenericOperationThreadCount());
    }

    @Test
    public void test_getRunningOperationCount() {
        initExecutor();

        executor.execute(new DummyOperation(-1).durationMs(2000));
        executor.execute(new DummyOperation(-1).durationMs(2000));

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
            executor.execute(new DummyOperation(-1).durationMs(2000));
        }
        for (int k = 0; k < executor.getPartitionOperationThreadCount(); k++) {
            executor.execute(new DummyOperation(k).durationMs(2000));
        }

        // now we throw in some work in the queues that wont' be picked up
        int count = 0;
        for (int l = 0; l < 3; l++) {
            for (int k = 0; k < executor.getGenericOperationThreadCount(); k++) {
                executor.execute(new DummyOperation(-1).durationMs(2000));
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

    @Test
    public void test_dumpPerformanceMetrics() {
        initExecutor();

        StringBuffer sb = new StringBuffer();
        executor.dumpPerformanceMetrics(sb);
        String content = sb.toString();


        assertTrue(content.contains("processedCount="));
    }
}

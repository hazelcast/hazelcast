package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionTaskFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_OPERATION_THREAD_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperationExecutorImpl_ExecuteBatchTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNullFactory() {
        initExecutor();

        executor.execute(null, new int[]{});
    }

    @Test(expected = NullPointerException.class)
    public void whenNullPartitions() {
        initExecutor();

        executor.execute(mock(PartitionTaskFactory.class), null);
    }

    @Test
    public void executeOnEachPartition() {
        config.setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "16");
        initExecutor();

        final int[] partitions = newPartitions();
        final DummyPartitionTaskFactory taskFactory = new DummyPartitionTaskFactory();
        executor.execute(taskFactory, partitions);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(partitions.length, taskFactory.completed.get());
            }
        });
    }

    @Test
    public void noMoreBubble() {
        config.setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "1");
        initExecutor();

        final DummyPartitionTaskFactory taskFactory = new DummyPartitionTaskFactory();
        taskFactory.delayMs = 1000;
        executor.execute(taskFactory, newPartitions());

        final DummyOperation op = new DummyOperation();
        executor.execute(op);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(op.completed);
            }
        }, 5);
    }

    @Test
    public void noCompletedBatchOperation_whenPartitionArrayIsEmpty() {
        config.setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "1");
        initExecutor();
        final LongGauge completedBatchOp = registerAndGetCompletedOperationBatchCountGauge();

        int[] emptyPartitionArray = {};
        executor.execute(new DummyPartitionTaskFactory(), emptyPartitionArray);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, completedBatchOp.read());
            }
        }, 5);
    }

    private LongGauge registerAndGetCompletedOperationBatchCountGauge() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(loggingService.getLogger(getClass()), ProbeLevel.INFO);
        LongGauge gauge = registry.newLongGauge("operation.thread[hz.hzName.partition-operation.thread-0].completedOperationBatchCount");
        executor.provideMetrics(registry);
        return gauge;
    }

    private int[] newPartitions() {
        final int[] partitions = new int[props.getInteger(PARTITION_OPERATION_THREAD_COUNT)];
        for (int k = 0; k < partitions.length; k++) {
            partitions[k] = k;
        }
        return partitions;
    }

    class DummyOperation extends Operation {
        private volatile boolean completed;

        @Override
        public void run() {
            completed = true;
        }
    }

    class DummyPartitionTaskFactory implements PartitionTaskFactory {
        private final AtomicLong completed = new AtomicLong();
        private int delayMs;

        @Override
        public Object create(int partitionId) {
            return new DummyTask(completed, delayMs);
        }
    }

    class DummyTask implements Runnable {
        private final AtomicLong completed;
        private final int delayMs;

        DummyTask(AtomicLong completed, int delayMs) {
            this.completed = completed;
            this.delayMs = delayMs;
        }

        @Override
        public void run() {
            sleepMillis(delayMs);
            completed.incrementAndGet();
        }
    }
}

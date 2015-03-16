package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicReference;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class StressTest {

    private MockOperationRunner runner;
    private ProgressiveScheduleQueue scheduleQueue;

    @Before
    public void setup() {
        runner = new MockOperationRunner(0);
    }

    @Test
    public void singlePartition() {
        PartitionThread workerThread = new PartitionThread();
        scheduleQueue = new ProgressiveScheduleQueue(workerThread);
        workerThread.scheduleQueue = scheduleQueue;
        PartitionQueue partitionQueue = new PartitionQueue(0, scheduleQueue, runner);

        for (int k = 0; k < 1000 * 1000; k++) {
            Operation op = new MockPartitionOperation();
            partitionQueue.add(op);
        }
    }

    public class PartitionThread extends Thread {
        private ProgressiveScheduleQueue scheduleQueue;
        private volatile boolean stop;
        private volatile Throwable cause;

        public PartitionThread() {
            setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    PartitionThread partitionThread = (PartitionThread) t;
                    partitionThread.cause = e;
                }
            });
        }

        public void run() {
            while (!stop) {
                try {
                    Object item = scheduleQueue.take();
                    runner.run((Operation) item);
                } catch (InterruptedException e) {

                }
            }
        }
    }
}

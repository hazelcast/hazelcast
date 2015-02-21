package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class PartitionQueue_runOrAddStressTest {

    public static final int THREAD_COUNT = 4;
    public static final int DURATION_MS = 30 * 1000;

    private PartitionQueue partitionQueue;
    private MockOperationRunner operationRunner;
    private ProgressiveScheduleQueue scheduleQueue;
    private Thread partitionThread;
    private AtomicBoolean stop = new AtomicBoolean();

    @Before
    public void setup() {
        partitionThread = new Thread();
        operationRunner = new MockOperationRunner(0);
        scheduleQueue = new ProgressiveScheduleQueue(partitionThread);
        partitionQueue = new PartitionQueue(0, scheduleQueue, operationRunner);
    }

    @Test
    public void test() throws InterruptedException {
        List<StressThread> stressThreads = new LinkedList<StressThread>();
        for (int k = 0; k < THREAD_COUNT; k++) {
            RunOrAddThread stressThread = new RunOrAddThread("stressthread-" + k);
            stressThreads.add(stressThread);
            stressThread.start();
        }

        int millis = DURATION_MS;
        Thread.sleep(millis);

        System.out.println("Waiting for threads to complete");
        stop.set(true);

        for (StressThread t : stressThreads) {
            t.join(10000);
            if (t.isAlive()) {
                System.out.println(partitionQueue.head.get());
                fail(format("thread %s failed to complete", t.getName()));
            }

            if (t.throwable != null) {
                System.out.println(partitionQueue.head.get());
                t.throwable.printStackTrace();
                fail(format("thread %s failed with an exception", t.getName()));
            }
        }

        Node node = partitionQueue.getHead();
        if (node.size() == 0) {
            assertSame(Node.UNPARKED, node);
        } else if (node.size() <= THREAD_COUNT) {
            assertEquals(PartitionQueueState.Unparked, node.state);
        } else {
            fail();
        }
    }

    public class RunOrAddThread extends StressThread {
        public RunOrAddThread(String name) {
            super(name);
        }

        @Override
        public void doRun() throws Throwable {
            int iteration = 0;
            while (!stop.get()) {
                PartitionOperation operation = new PartitionOperation();
                partitionQueue.runOrAdd(operation);
                operation.get();

                iteration++;
                if (iteration % 100000 == 0) {
                    System.out.println(getName() + " is at:" + iteration);
                }
            }

            partitionQueue.runOrAdd(new PartitionOperation());
        }
    }
}

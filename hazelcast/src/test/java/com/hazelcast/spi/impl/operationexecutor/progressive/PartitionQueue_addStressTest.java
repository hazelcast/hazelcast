package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * This test verifies the behavior of multiple threads doing add on the PartitionQueue and a single thread doing takes on
 * the ProgressiveScheduleQueue.
 * <p/>
 * This behavior is like using the system as a normal queue where you have multiple producers and a single consumer. No
 * fancy stuff like caller-runs or priorities.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class PartitionQueue_addStressTest {

    public static final int THREAD_COUNT = 4;
    public static final int DURATION_MS = 30 * 1000;
    public static final int MAX_ADD_BURST = 100;
    //number of partitions handled by the operation-thread.
    public static final int PARTITION_COUNT = 4;

    public int priorityPercentage = 0;
    private PartitionQueue[] partitionQueues;
    private MockOperationRunner operationRunner;
    private ProgressiveScheduleQueue scheduleQueue;
    private TakeOperationThread partitionThread;
    private final AtomicBoolean stop = new AtomicBoolean();

    @Before
    public void setup() {
        partitionThread = new TakeOperationThread();
        operationRunner = new MockOperationRunner(0);
        scheduleQueue = new ProgressiveScheduleQueue(partitionThread);

        partitionQueues = new PartitionQueue[PARTITION_COUNT];
        for (int partitionId = 0; partitionId < partitionQueues.length; partitionId++) {
            partitionQueues[partitionId] = new PartitionQueue(partitionId, scheduleQueue, operationRunner);
        }
    }

    @Test
    public void noPriority() throws InterruptedException {
        priorityPercentage = 0;
        test();
    }

    @Test
    public void somePriorities() throws InterruptedException {
        priorityPercentage = 10;
        test();
    }

    @Test
    public void onlyPriority() throws InterruptedException {
        priorityPercentage = 100;
        test();
    }

    public void test() throws InterruptedException {
        List<AddThread> addThreads = new LinkedList<AddThread>();
        for (int k = 0; k < THREAD_COUNT; k++) {
            AddThread addThread = new AddThread("AddThread-" + k);
            addThreads.add(addThread);
            addThread.start();
        }

        partitionThread.start();
        new ScanThread().start();

        int millis = DURATION_MS;
        Thread.sleep(millis);

        System.out.println("Waiting for threads to complete");
        stop.set(true);

        for (AddThread t : addThreads) {
            join(t);
        }

        partitionThread.interrupt();

        for (PartitionQueue partitionQueue : partitionQueues) {
            Node node = partitionQueue.getHead();
            assertSame(Node.PARKED, node);
        }
    }


    private void join(StressThread t) throws InterruptedException {
        t.join(10000);
        if (t.isAlive()) {
            String stacktrace = stackTraceToString(t);
            System.out.println(stacktrace);
            //System.out.println(partitionQueue.head.get());
            fail(format("thread %s failed to complete", t.getName()));
        }

        if (t.throwable != null) {
            //System.out.println(partitionQueue.head.get());
            t.throwable.printStackTrace();
            fail(format("thread %s failed with an exception", t.getName()));
        }
    }

    public String stackTraceToString(Thread t) {
        StringBuilder sb = new StringBuilder();
        sb.append(t.getName() + "\n");
        for (StackTraceElement element : t.getStackTrace()) {
            sb.append("\t" + element.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

    public class ScanThread extends Thread {
        public void run() {
            while (true) {

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                System.out.println("----------------------------");
                for (PartitionQueue partitionQueue : partitionQueues) {
                    System.out.println(partitionQueue);
                }
                System.out.println(scheduleQueue);

            }
        }
    }

    public class TakeOperationThread extends StressThread {
        public TakeOperationThread() {
            super("TakeThread");
        }

        @Override
        public void doRun() throws Throwable {
            int iteration = 0;

            while (true) {
                Operation task;
                try {
                    task = (Operation) scheduleQueue.take();
                } catch (InterruptedException e) {
                    System.out.println(getName() + " is interrupted, exiting");
                    return;
                }

                process(task);

                iteration++;
                if (iteration % 100000 == 0) {
                    System.out.println(getName() + " is at:" + iteration);
                }
            }
        }

        private void process(Object task) {
            Operation op = (Operation) task;
            operationRunner.run(op);
        }
    }


    public class AddThread extends StressThread {

        private final Random random = new Random();
        private final AtomicLong[] sequences = new AtomicLong[PARTITION_COUNT];
        private final long[] expectedSequences = new long[PARTITION_COUNT];
        private long iteration = 0;

        private final List<TestOperation> operations = new ArrayList<TestOperation>();

        public AddThread(String name) {
            super(name);

            for (int k = 0; k < sequences.length; k++) {
                sequences[k] = new AtomicLong();
            }

        }

        @Override
        public void doRun() throws Throwable {
            while (!stop.get()) {
                int burst = Math.max(1, random.nextInt(MAX_ADD_BURST));

                int partitionId = random.nextInt(PARTITION_COUNT);
                for (int k = 0; k < burst; k++) {
                    boolean priority = randomPriority();

                    AtomicLong sequence = sequences[partitionId];
                    long expectedSequence = expectedSequences[partitionId];
                    expectedSequences[partitionId]++;

                    PartitionQueue partitionQueue = partitionQueues[partitionId];

                    TestOperation operation = new TestOperation(partitionId, expectedSequence, sequence);

                    if (priority) {
                        partitionQueue.priorityAdd(operation);
                    } else {
                        partitionQueue.runOrAdd(operation);
                    }

                    operations.add(operation);

                    iteration++;
                    if (iteration % 100000 == 0) {
                        System.out.println(getName() + " is at:" + iteration);
                    }
                }

                for (TestOperation operation : operations) {
                    operation.get();
                }
                operations.clear();
            }

            System.out.println(getName() + " completed");
        }

        boolean randomPriority() {
            if (priorityPercentage == 0) {
                return false;
            }
            if (priorityPercentage == 100) {
                return true;
            }

            return random.nextInt(100) < priorityPercentage;

        }
    }

    public class TestOperation extends PartitionOperation {
        private final long expectedCallSequence;
        private final AtomicLong sequence;


        public TestOperation(int partitionId, long expectedCallSequence, AtomicLong sequence) {
            super(partitionId);
            this.expectedCallSequence = expectedCallSequence;
            this.sequence = sequence;
        }

        @Override
        public Object call() throws Throwable {
//            long foundSequence = sequence.getAndIncrement();
//            assertEquals(expectedCallSequence, foundSequence);
            return null;
        }
    }

}

/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.impl.executor;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.StandardLoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test verifies the following:
 * <ol>
 * <li>when a task is offered, is should be executed.  </li>
 * <li>when a task is offered to a segment, it should be executed in the order it was offered. </li>
 * </ol>
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ParallelExecutorServiceImplStressTest {

    public static final int TASK_DURATION_MS = 50;
    public static final int POOL_SIZE = 10;
    public static final int CONCURRENCY_LEVEL = 20;
    public static final int TASK_COUNT = 10 * 1000;

    private AtomicLong violationCounter;
    private CountDownLatch completedLatch;
    private ThreadPoolExecutor executorService;
    private ParallelExecutorService parallelExecutorService;

    @Before
    public void setUp() {
        ILogger logger = new StandardLoggerFactory().getLogger(ParallelExecutorServiceImplStressTest.class.getName());
        executorService = new ThreadPoolExecutor(POOL_SIZE, POOL_SIZE, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        parallelExecutorService = new ParallelExecutorService(logger, executorService);
        violationCounter = new AtomicLong();
        completedLatch = new CountDownLatch(TASK_COUNT);
    }

    @After
    public void tearDown() throws InterruptedException {
        executorService.shutdownNow();
        assertTrue("ExecutorService failed to terminate within timeout window", executorService.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    public void testBlocking() throws Exception {
        test(10);
    }

    @Test
    public void testNonBlocking() throws Exception {
        test(Integer.MAX_VALUE);
    }

    /**
     * @param capacity the capacity of the blockingqueue per segment. Integer.MAX_VALUE indicates unbound capacity.
     */
    public void test(int capacity) throws Exception {
        ParallelExecutor executor = parallelExecutorService.newBlockingParallelExecutor(CONCURRENCY_LEVEL, capacity);
        Random random = new Random();
        Segment[] segmentArray = new Segment[CONCURRENCY_LEVEL];
        for (int k = 0; k < segmentArray.length; k++) {
            segmentArray[k] = new Segment(k);
        }
        for (int k = 0; k < TASK_COUNT; k++) {
            sleepMs(random.nextInt(2));
            int segmentIndex = random.nextInt(CONCURRENCY_LEVEL);
            Segment segment = segmentArray[segmentIndex];
            TestRunnable runnable = new TestRunnable(segment);
            executor.execute(runnable, segmentIndex);
        }
        //wait for the tasks to complete. If, for whatever reason, a task is forgotten to be executed, this statement
        //is going to fail. 
        assertTrue("The tasks where not executed in the given timeout", completedLatch.await(120, TimeUnit.SECONDS));
        //make sure that no violations have taken place.
        assertEquals(violationCounter.get(), 0);
    }

    static class Segment {
        final long segment;
        final AtomicLong activeCounter = new AtomicLong();
        final AtomicLong sequenceIdGenerator = new AtomicLong();
        final AtomicLong expectedSequenceNumber = new AtomicLong();
        final Random random = new Random();

        Segment(long segment) {
            this.segment = segment;
        }
    }

    class TestRunnable implements Runnable {
        private final Segment segment;
        private final long sequenceNumber;

        TestRunnable(Segment segment) {
            this.segment = segment;
            this.sequenceNumber = segment.sequenceIdGenerator.getAndIncrement();
        }

        public void run() {
            if (segment.activeCounter.getAndIncrement() != 0) {
                System.out.println("ERROR: Concurrent execution within a segment has taken place");
                violationCounter.incrementAndGet();
            }
            if (segment.expectedSequenceNumber.getAndIncrement() != sequenceNumber) {
                System.out.println("ERROR: An out of order execution within a segment has taken place");
                violationCounter.incrementAndGet();
            }
            if (sequenceNumber % 100 == 0) {
                System.out.println("Segment [" + segment.segment + "] is at element [" + sequenceNumber + "]");
            }
            sleepMs(segment.random.nextInt(TASK_DURATION_MS));
            segment.activeCounter.decrementAndGet();
            completedLatch.countDown();
        }
    }

    static void sleepMs(long period) {
        try {
            Thread.sleep(period);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

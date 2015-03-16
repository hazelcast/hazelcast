package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProgressiveScheduleQueue_takeTest extends ProgressiveScheduleQueueAbstractTest {

    @Test(expected = IllegalStateException.class)
    public void whenWrongThread_thenIllegalStateException() throws InterruptedException {
        Thread thread = new Thread();
        scheduleQueue = new ProgressiveScheduleQueue(thread);
        scheduleQueue.take();
    }

    @Test
    public void test1() throws InterruptedException {
        PartitionQueue p1 = new PartitionQueue(0, scheduleQueue, operationRunner);

        MockPartitionOperation op1 = new MockPartitionOperation();
        MockPartitionOperation op2 = new MockPartitionOperation();
        p1.add(op1);
        p1.add(op2);

        Object found1 = scheduleQueue.take();
        assertSame(op1, found1);

        Object found2 = scheduleQueue.take();
        assertSame(op2, found2);
    }

    @Test
    public void test2() throws InterruptedException {
        ProgressiveScheduleQueue scheduleQueue = new ProgressiveScheduleQueue(Thread.currentThread());
        PartitionQueue p1 = new PartitionQueue(0, scheduleQueue,
                operationRunner);
        PartitionQueue p2 = new PartitionQueue(0, scheduleQueue,
                operationRunner);

        MockPartitionOperation op1 = new MockPartitionOperation();
        MockPartitionOperation op2 = new MockPartitionOperation();
        p1.add(op1);
        Object found1 = scheduleQueue.take();
        assertSame(op1, found1);

        p2.add(op2);
        Object found2 = scheduleQueue.take();
        assertSame(op2, found2);
    }
}

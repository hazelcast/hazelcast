package com.hazelcast.spi.impl.operationexecutor.progressive;


import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProgressiveScheduleQueue_appendToBuffersTest extends ProgressiveScheduleQueueAbstractTest{

    @Test
    public void singleNormalItem() {
        PartitionQueue pq = new PartitionQueue(0, scheduleQueue, operationRunner);

        UnparkNode node1 = new UnparkNode(pq, false);
        node1.normalSize = 1;

        scheduleQueue.appendToBuffers(node1);

        assertEquals(1, scheduleQueue.bufferSize);
        assertEquals(0, scheduleQueue.bufferPos);

        assertSame(pq, scheduleQueue.buffer[0]);

        assertEquals(0, scheduleQueue.priorityBufferSize);
        assertEquals(0, scheduleQueue.priorityBufferPos);
    }

    @Test
    public void multipleNormalItem() {
        Chain chain = new Chain();
        UnparkNode node = chain.create(10 * ProgressiveScheduleQueue.INITIAL_BUFFER_CAPACITY);

        scheduleQueue.appendToBuffers(node);

        assertContent(chain);
        assertEquals(0, scheduleQueue.priorityBufferSize);
        assertEquals(0, scheduleQueue.priorityBufferPos);
    }

    @Test
    public void appendMultipleNormalItems() {
        Chain chain = new Chain();
        UnparkNode node1 = chain.create(10);
        UnparkNode node2 = chain.create(100);

        scheduleQueue.appendToBuffers(node1);
        scheduleQueue.appendToBuffers(node2);

        assertContent(chain);
        assertEquals(0, scheduleQueue.priorityBufferSize);
        assertEquals(0, scheduleQueue.priorityBufferPos);
    }

    @Test
    public void singlePriorityItem() {
        PartitionQueue pq = new PartitionQueue(0, scheduleQueue, operationRunner);

        UnparkNode node1 = new UnparkNode(pq, true);
        node1.prioritySize = 1;

        scheduleQueue.appendToBuffers(node1);

        assertEquals(1, scheduleQueue.priorityBufferSize);
        assertEquals(0, scheduleQueue.priorityBufferPos);
        assertSame(pq, scheduleQueue.priorityBuffer[0]);
        assertEquals(0, scheduleQueue.bufferSize);
        assertEquals(0, scheduleQueue.bufferPos);
    }

    @Test
    public void priority_growining() {
        Chain chain = new Chain();
        UnparkNode node = chain.withPriority().create(10 * ProgressiveScheduleQueue.INITIAL_BUFFER_CAPACITY);

        scheduleQueue.appendToBuffers(node);

        assertContent(chain);
    }

    @Test
    public void mixture() {
        Chain chain = new Chain().withRandomPriority();

        for (int k = 1; k < 300; k++) {
            UnparkNode node = chain.create(k);
            scheduleQueue.appendToBuffers(node);
            assertContent(chain);
        }
    }

    private void assertContent(Chain chain) {
        List<PartitionQueue> queues = chain.queues;
        assertEquals(chain.count, scheduleQueue.bufferSize);
        assertEquals(chain.priorityCount, scheduleQueue.priorityBufferSize);
        int normalIndex = 0;
        int priorityIndex = 0;
        for (int k = 0; k < queues.size(); k++) {
            PartitionQueue expected = queues.get(k);
            boolean isPriority = chain.priorityMap.get(expected);
            if (isPriority) {
                PartitionQueue actual = (PartitionQueue) scheduleQueue.priorityBuffer[priorityIndex];
                assertSame("wrong partition queue found at priority index:" + priorityIndex, expected, actual);
                priorityIndex++;
            } else {
                PartitionQueue actual = (PartitionQueue) scheduleQueue.buffer[normalIndex];
                assertSame("wrong partition queue found at index:" + normalIndex, expected, actual);
                normalIndex++;
            }
        }
    }

    private class Chain {
        List<PartitionQueue> queues = new ArrayList<PartitionQueue>();
        Map<PartitionQueue, Boolean> priorityMap = new HashMap<PartitionQueue, Boolean>();
        int priorityCount;
        int count;
        Random random = new Random();

        Boolean priority = Boolean.FALSE;

        public Chain withPriority() {
            this.priority = true;
            return this;
        }

        public Chain withRandomPriority() {
            this.priority = null;
            return this;
        }

        public UnparkNode create(int size) {
            UnparkNode node = null;
            for (int k = 0; k < size; k++) {
                PartitionQueue pq = new PartitionQueue(0, scheduleQueue, operationRunner);
                queues.add(pq);

                boolean hasPriority;
                if (priority == null) {
                    hasPriority = random.nextBoolean();
                } else {
                    hasPriority = priority;
                }

                UnparkNode newNode = new UnparkNode(pq, hasPriority);

                if (hasPriority) {
                    newNode.prioritySize = node == null ? 1 : node.prioritySize + 1;
                    newNode.normalSize = node == null ? 0 : node.normalSize;
                    priorityCount++;
                } else {
                    newNode.prioritySize = node == null ? 0 : node.prioritySize;
                    newNode.normalSize = node == null ? 1 : node.normalSize + 1;
                    count++;
                }

                this.priorityMap.put(pq, hasPriority);

                newNode.prev = node;
                node = newNode;
            }
            return node;
        }
    }
}

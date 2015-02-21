package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NodeTest {

    @Test
    public void init() {
        Node prev = new Node();
        prev.normalSize = 10;
        prev.prioritySize = 20;

        Node node = new Node();
        Object task = "";
        node.init(task, PartitionQueueState.Parked, prev);

        assertEquals(prev.normalSize + 1, node.normalSize);
        assertEquals(prev.prioritySize, node.prioritySize);
        assertEquals(task, node.task);
        assertFalse(node.hasPriority);
        assertSame(prev, node.prev);
        assertEquals(PartitionQueueState.Parked, node.state);
    }

    @Test
    public void init_whenNullTask() {
        Node node = new Node();
        node.init(null, PartitionQueueState.Parked, null);

        assertEquals(0, node.prioritySize);
        assertEquals(0, node.normalSize);
        assertEquals(null, node.task);
        assertFalse(node.hasPriority);
        assertNull(node.prev);
        assertEquals(PartitionQueueState.Parked, node.state);
    }

    @Test
    public void priorityInit() {
        Node prev = new Node();
        prev.normalSize = 10;
        prev.prioritySize = 20;

        Node node = new Node();
        Object task = "";
        node.priorityInit(task, PartitionQueueState.Parked, prev);

        assertEquals(prev.normalSize, node.normalSize);
        assertEquals(prev.prioritySize + 1, node.prioritySize);
        assertEquals(task, node.task);
        assertTrue(node.hasPriority);
        assertSame(prev, node.prev);
        assertEquals(PartitionQueueState.Parked, node.state);
    }

    @Test
    public void priorityInit_whenNullTask() {
        Node node = new Node();
        node.priorityInit(null, PartitionQueueState.Parked, null);

        assertEquals(0, node.prioritySize);
        assertEquals(0, node.normalSize);
        assertEquals(null, node.task);
        assertTrue(node.hasPriority);
        assertNull(node.prev);
        assertEquals(PartitionQueueState.Parked, node.state);
    }
}

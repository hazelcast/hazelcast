package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.PARKED;
import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.UNPARKED;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Executing;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.UnparkedPriority;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionQueue_pollTest extends PartitionQueueAbstractTest {

    @Test
    public void whenParked_thenThrowsIllegalStateException() {
        whenIllegalState_thenIllegalStateException(PARKED);
    }

    @Test
    public void whenUnparked_thenThrowsIllegalStateException() {
        whenIllegalState_thenIllegalStateException(UNPARKED);
    }

    @Test
    public void whenPriorityUnparked_thenThrowsIllegalStateException() {
        whenIllegalState_thenIllegalStateException(new Node(UnparkedPriority));
    }

    public void whenIllegalState_thenIllegalStateException(Node state) {
        partitionQueue.head.set(state);

        beforeTest();
        try {
            partitionQueue.poll(true);
            fail();
        } catch (IllegalStateException expected) {
        }

        assertHead(state);
        assertNoNewUnparks();
    }

    @Test
    public void whenExecuting() {
        test(Executing);
    }

    //todo: a lot more testing is needed.

    public void test(PartitionQueueState state) {
        Node node = new Node();
        node.state = state;

        partitionQueue.head.set(node);

        beforeTest();
        Object value = partitionQueue.poll(false);
        assertNull(value);

        node = new Node();
        node.state = state;
        node.normalSize = 1;
    }
}

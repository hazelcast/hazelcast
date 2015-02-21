package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.PARKED;
import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.STOLEN;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionQueue_runAndDropTest extends PartitionQueueAbstractTest {

    @Test
    public void test() {
        DummyOperation op = new DummyOperation();

        Node node = new Node();
        node.normalSize = 1;
        node.task = op;
        node.state = PartitionQueueState.Stolen;

        partitionQueue.head.set(STOLEN);

        partitionQueue.runAndDrop(node, null);

        assertHead(PARKED);
        //assertEquals(1, dummyOpe)
    }
}

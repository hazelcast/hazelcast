package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.impl.RemoteOperationResponseHandler;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.RemoteOperationResponseHandler.buildResponsePacket;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ClassicOperationExecutor#execute(com.hazelcast.nio.Packet)}
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExecutePacketTest extends AbstractClassicOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNullPacket() {
        initExecutor();

        executor.execute((Packet) null);
    }

    @Test
    public void test_whenResponsePacket() {
        initExecutor();

        final Packet packet = buildResponsePacket(serializationService, false, 1, 0, null);
        executor.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyResponsePacketHandler responsePacketHandler = (DummyResponsePacketHandler) ExecutePacketTest.this.responsePacketHandler;
                responsePacketHandler.packets.contains(packet);
            }
        });
    }

    @Test
    public void test_whenPartitionSpecificOperationPacket() {
        initExecutor();

        final DummyOperation operation = new DummyOperation(0);
        final Packet packet = new Packet(serializationService.toBytes(operation), operation.getPartitionId());
        packet.setFlag(Packet.FLAG_OP);
        executor.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                OperationRunner[] partitionHandlers = executor.getPartitionOperationRunners();
                DummyOperationRunner handler = (DummyOperationRunner) partitionHandlers[operation.getPartitionId()];
                assertTrue(handler.packets.contains(packet));
            }
        });
    }

    @Test
    public void test_whenGenericOperationPacket() {
        initExecutor();

        final DummyOperation operation = new DummyOperation(Operation.GENERIC_PARTITION_ID);
        final Packet packet = new Packet(serializationService.toBytes(operation), operation.getPartitionId());
        packet.setFlag(Packet.FLAG_OP);
        executor.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                OperationRunner[] genericHandlers = executor.getGenericOperationRunners();
                boolean found = false;
                for (OperationRunner h : genericHandlers) {
                    DummyOperationRunner dummyOperationHandler = (DummyOperationRunner) h;
                    if (dummyOperationHandler.packets.contains(packet)) {
                        found = true;
                        break;
                    }
                }
                assertTrue("Packet is not found on any of the generic handlers", found);
            }
        });
    }
}

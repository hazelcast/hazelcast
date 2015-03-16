package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NormalResponse;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProgressiveOperationExecutor_executePacketTest extends AbstractProgressiveOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void whenNullPacket_thenNullPointerException() {
        initExecutor();

        executor.execute((Packet) null);
    }

    @Test
    public void whenResponsePacket_thenResponsePacketHandlerInvoked() {
        initExecutor();

        final NormalResponse normalResponse = new NormalResponse(null, 1, 0, false);
        Data data = serializationService.toData(normalResponse);
        final Packet packet = new Packet(data, 0, serializationService.getPortableContext());
        packet.setHeader(Packet.HEADER_RESPONSE);
        packet.setHeader(Packet.HEADER_OP);
        executor.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyResponsePacketHandler responsePacketHandler = (DummyResponsePacketHandler) ProgressiveOperationExecutor_executePacketTest.this.responsePacketHandler;
                responsePacketHandler.packets.contains(packet);
                responsePacketHandler.responses.contains(normalResponse);
            }
        });
    }

    @Test
    public void whenGenericOperationPacket_andNoPriority_thenProcessedOnGenericThread() {
        whenGenericOperationPacket_thenProcessedOnGenericThread(false);
    }

    @Test
    public void whenGenericOperationPacket_andPriority_thenProcessedOnGenericThread() {
        whenGenericOperationPacket_thenProcessedOnGenericThread(true);
    }

    public void whenGenericOperationPacket_thenProcessedOnGenericThread(boolean priority) {
        initExecutor();

        final GenericOperation op = new GenericOperation();
        Data data = serializationService.toData(op);
        final Packet packet = new Packet(data, op.getPartitionId(), serializationService.getPortableContext());
        packet.setHeader(Packet.HEADER_OP);
        if (priority) {
            packet.setHeader(Packet.HEADER_URGENT);
        }

        executor.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyOperationRunnerFactory rf = (DummyOperationRunnerFactory) runnerFactory;
                boolean found = false;
                for (DummyOperationRunner runner : rf.genericRunners) {
                    if (runner.packets.contains(packet)) {
                        found = true;
                        break;
                    }
                }

                assertTrue("packet should be found on a one of the generic runners", found);
                //todo: we need to verify that the operation is run on a generic thread.
            }
        });
    }

    @Test
    public void whenPartitionOperationPacket_andPriority_thenProcessedOnPartitionThread() {
        whenPartitionOperationPacket_thenProcessedOnPartitionThread(true);
    }

    @Test
    public void whenPartitionOperationPacket_andNoPriority_thenProcessedOnPartitionThread() {
        whenPartitionOperationPacket_thenProcessedOnPartitionThread(false);
    }

    public void whenPartitionOperationPacket_thenProcessedOnPartitionThread(boolean priority) {
        initExecutor();

        final PartitionOperation op = new PartitionOperation();
        Data data = serializationService.toData(op);
        final Packet packet = new Packet(data, op.getPartitionId(), serializationService.getPortableContext());
        packet.setHeader(Packet.HEADER_OP);
        if (priority) {
            packet.setHeader(Packet.HEADER_URGENT);
        }

        executor.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyOperationRunnerFactory rf = (DummyOperationRunnerFactory) runnerFactory;
                DummyOperationRunner runner = rf.partitionRunners.get(op.getPartitionId());

                assertTrue("packet should be found in the correct partition runner", runner.packets.contains(packet));

                DummyOperation deserializedOp = (DummyOperation) runner.getOperation(packet);

                Assert.assertNotNull(deserializedOp);

                assertExecutedByPartitionThread(deserializedOp);
                // todo: we need to verify that the operation has been run on the correct partition-thread
            }
        });
    }
}

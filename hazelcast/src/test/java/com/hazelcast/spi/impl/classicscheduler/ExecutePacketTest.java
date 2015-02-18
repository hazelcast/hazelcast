package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NormalResponse;
import com.hazelcast.spi.impl.OperationHandler;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ClassicOperationScheduler#execute(com.hazelcast.nio.Packet)}
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExecutePacketTest extends AbstractClassicSchedulerTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNullPacket() {
        initScheduler();

        scheduler.execute((Packet) null);
    }

    @Test
    public void test_whenResponsePacket() {
        initScheduler();

        final NormalResponse normalResponse = new NormalResponse(null, 1, 0, false);
        Data data = serializationService.toData(normalResponse);
        final Packet packet = new Packet(data, 0, serializationService.getPortableContext());
        packet.setHeader(Packet.HEADER_RESPONSE);
        packet.setHeader(Packet.HEADER_OP);
        scheduler.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyResponsePacketHandler responsePacketHandler = (DummyResponsePacketHandler)ExecutePacketTest.this.responsePacketHandler;
                responsePacketHandler.packets.contains(packet);
                responsePacketHandler.responses.contains(normalResponse);
            }
        });
    }

    @Test
    public void test_whenPartitionSpecificOperationPacket() {
        initScheduler();

        final DummyOperation operation = new DummyOperation(0);
        Data data = serializationService.toData(operation);
        final Packet packet = new Packet(data, operation.getPartitionId(), serializationService.getPortableContext());
        packet.setHeader(Packet.HEADER_OP);
        scheduler.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                OperationHandler[] partitionHandlers = scheduler.getPartitionOperationHandlers();
                DummyOperationHandler handler = (DummyOperationHandler) partitionHandlers[operation.getPartitionId()];
                assertTrue(handler.packets.contains(packet));
            }
        });
    }

    @Test
    public void test_whenGenericOperationPacket() {
        initScheduler();

        final DummyOperation operation = new DummyOperation(-1);
        Data data = serializationService.toData(operation);
        final Packet packet = new Packet(data, operation.getPartitionId(), serializationService.getPortableContext());
        packet.setHeader(Packet.HEADER_OP);
        scheduler.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                OperationHandler[] genericHandlers = scheduler.getGenericOperationHandlers();
                boolean found = false;
                for (OperationHandler h : genericHandlers) {
                    DummyOperationHandler dummyOperationHandler = (DummyOperationHandler) h;
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

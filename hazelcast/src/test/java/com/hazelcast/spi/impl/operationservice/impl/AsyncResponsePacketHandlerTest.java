package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AsyncResponsePacketHandlerTest extends HazelcastTestSupport {

    private PacketHandler responsePacketHandler;
    private AsyncResponsePacketHandler asyncHandler;
    private SerializationService serializationService;

    @Before
    public void setup() {
        ILogger logger = Logger.getLogger(getClass());
        HazelcastThreadGroup threadGroup = new HazelcastThreadGroup("test", logger, getClass().getClassLoader());
        responsePacketHandler = mock(PacketHandler.class);
        asyncHandler = new AsyncResponsePacketHandler(threadGroup, logger, responsePacketHandler);
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void whenNoProblemPacket() throws Exception {
        final Packet packet = new Packet(serializationService.toBytes(new NormalResponse("foo", 1, 0, false)));
        packet.setFlag(Packet.FLAG_OP);
        packet.setFlag(Packet.FLAG_RESPONSE);
        asyncHandler.handle(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(responsePacketHandler).handle(packet);
            }
        });
    }

    @Test
    public void whenPacketThrowsException() throws Exception {
        final Packet badPacket = new Packet(serializationService.toBytes(new NormalResponse("bad", 1, 0, false)));
        badPacket.setFlag(Packet.FLAG_OP);
        badPacket.setFlag(Packet.FLAG_RESPONSE);

        final Packet goodPacket = new Packet(serializationService.toBytes(new NormalResponse("good", 1, 0, false)));
        goodPacket.setFlag(Packet.FLAG_OP);
        goodPacket.setFlag(Packet.FLAG_RESPONSE);

        doThrow(new ExpectedRuntimeException()).when(responsePacketHandler).handle(badPacket);

        asyncHandler.handle(badPacket);
        asyncHandler.handle(goodPacket);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(responsePacketHandler).handle(goodPacket);
            }
        });
    }

    @Test
    public void whenShutdown(){
        asyncHandler.shutdown();

        final Packet packet = new Packet(serializationService.toBytes(new NormalResponse("foo", 1, 0, false)));
        packet.setFlag(Packet.FLAG_OP);
        packet.setFlag(Packet.FLAG_RESPONSE);
        asyncHandler.handle(packet);

        assertTrueFiveSeconds(new AssertTask() {
            @Override
            public void run() throws Exception {
                verifyZeroInteractions(responsePacketHandler);
            }
        });
    }
}

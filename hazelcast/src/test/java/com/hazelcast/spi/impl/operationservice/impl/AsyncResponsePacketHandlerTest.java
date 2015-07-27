package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
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
@Category(QuickTest.class)
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
    public void test() throws Exception {
        final Packet packet = new Packet(serializationService.toData(new NormalResponse("foo", 1, 0, false)));
        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);
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
        final Packet badPacket = new Packet(serializationService.toData(new NormalResponse("foo", 1, 0, false)));
        badPacket.setHeader(Packet.HEADER_OP);
        badPacket.setHeader(Packet.HEADER_RESPONSE);

        final Packet goodPacket = new Packet(serializationService.toData(new NormalResponse("foo", 1, 0, false)));
        goodPacket.setHeader(Packet.HEADER_OP);
        goodPacket.setHeader(Packet.HEADER_RESPONSE);

        doThrow(new Exception()).when(responsePacketHandler).handle(badPacket);

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

        final Packet packet = new Packet(serializationService.toData(new NormalResponse("foo", 1, 0, false)));
        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);
        asyncHandler.handle(packet);

        assertTrueFiveSeconds(new AssertTask() {
            @Override
            public void run() throws Exception {
                verifyZeroInteractions(responsePacketHandler);
            }
        });
    }
}

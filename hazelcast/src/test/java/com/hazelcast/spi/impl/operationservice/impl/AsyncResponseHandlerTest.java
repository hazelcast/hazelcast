package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AsyncResponseHandlerTest extends HazelcastTestSupport {

    private PacketHandler responsePacketHandler;
    private AsyncResponseHandler asyncHandler;
    private InternalSerializationService serializationService;

    @Before
    public void setup() {
        ILogger logger = Logger.getLogger(getClass());
        HazelcastThreadGroup threadGroup = new HazelcastThreadGroup("test", logger, getClass().getClassLoader());
        responsePacketHandler = mock(PacketHandler.class);
        asyncHandler = new AsyncResponseHandler(threadGroup, logger, responsePacketHandler);
        asyncHandler.start();
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void whenNoProblemPacket() throws Exception {
        final Packet packet = new Packet(serializationService.toBytes(new NormalResponse("foo", 1, 0, false)));
        packet.setFlag(FLAG_OP);
        packet.setFlag(FLAG_RESPONSE);
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
        badPacket.setFlag(FLAG_OP);
        badPacket.setFlag(FLAG_RESPONSE);

        final Packet goodPacket = new Packet(serializationService.toBytes(new NormalResponse("good", 1, 0, false)));
        goodPacket.setFlag(FLAG_OP);
        goodPacket.setFlag(FLAG_RESPONSE);

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
    @Ignore //https://github.com/hazelcast/hazelcast/issues/7864
    public void whenShutdown() {
        asyncHandler.shutdown();

        final Packet packet = new Packet(serializationService.toBytes(new NormalResponse("foo", 1, 0, false)));
        packet.setFlag(FLAG_OP);
        packet.setFlag(FLAG_RESPONSE);
        asyncHandler.handle(packet);

        assertTrueFiveSeconds(new AssertTask() {
            @Override
            public void run() throws Exception {
                verifyZeroInteractions(responsePacketHandler);
            }
        });
    }
}

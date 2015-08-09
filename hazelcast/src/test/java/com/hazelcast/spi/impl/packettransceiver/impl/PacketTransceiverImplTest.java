package com.hazelcast.spi.impl.packettransceiver.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PacketTransceiverImplTest extends HazelcastTestSupport {

    private PacketHandler operationPacketHandler;
    private PacketHandler eventPacketHandler;
    private PacketTransceiverImpl transceiver;
    private PacketHandler wanPacketHandler;
    private PacketHandler connectionManagerPacketHandler;
    private ExecutionService executionService;

    @Before
    public void setup() {
        ILogger logger = Logger.getLogger(getClass());
        operationPacketHandler = mock(PacketHandler.class);
        eventPacketHandler = mock(PacketHandler.class);
        wanPacketHandler = mock(PacketHandler.class);
        connectionManagerPacketHandler = mock(PacketHandler.class);
        executionService = mock(ExecutionService.class);
        transceiver = new PacketTransceiverImpl(
                null,
                logger,
                executionService, operationPacketHandler,
                eventPacketHandler,
                wanPacketHandler,
                connectionManagerPacketHandler
        );
    }

    @Test
    public void whenOperationPacket() throws Exception {
        Packet packet = new Packet();
        packet.setHeader(Packet.HEADER_OP);

        transceiver.receive(packet);

        verify(operationPacketHandler).handle(packet);
        verifyZeroInteractions(eventPacketHandler);
        verifyZeroInteractions(wanPacketHandler);
        verifyZeroInteractions(connectionManagerPacketHandler);
    }

    @Test
    public void whenWanReplicationPacket() throws Exception {
        Packet packet = new Packet();
        packet.setHeader(Packet.HEADER_WAN_REPLICATION);

        transceiver.receive(packet);

        verify(wanPacketHandler).handle(packet);
        verifyZeroInteractions(operationPacketHandler);
        verifyZeroInteractions(eventPacketHandler);
        verifyZeroInteractions(connectionManagerPacketHandler);
    }

    @Test
    public void whenEventPacket() throws Exception {
        Packet packet = new Packet();
        packet.setHeader(Packet.HEADER_EVENT);

        transceiver.receive(packet);

        verify(eventPacketHandler).handle(packet);
        verifyZeroInteractions(operationPacketHandler);
        verifyZeroInteractions(wanPacketHandler);
        verifyZeroInteractions(connectionManagerPacketHandler);
    }

    @Test
    public void whenBindPacket() throws Exception {
        Packet packet = new Packet();
        packet.setHeader(Packet.HEADER_BIND);

        transceiver.receive(packet);

        verify(connectionManagerPacketHandler).handle(packet);
        verifyZeroInteractions(operationPacketHandler);
        verifyZeroInteractions(eventPacketHandler);
        verifyZeroInteractions(wanPacketHandler);
    }

    // unrecognized packets are logged. No handlers is contacted.
    @Test
    public void whenUnrecognizedPacket_thenSwallowed() throws Exception {
        Packet packet = new Packet();

        transceiver.receive(packet);

        verifyZeroInteractions(connectionManagerPacketHandler);
        verifyZeroInteractions(operationPacketHandler);
        verifyZeroInteractions(eventPacketHandler);
        verifyZeroInteractions(wanPacketHandler);
    }

    // when one of the handlers throws an exception, the exception is logged but not rethrown
    @Test
    public void whenProblemHandlingPacket_thenSwallowed() throws Exception {
        Packet packet = new Packet();
        packet.setHeader(Packet.HEADER_OP);

        Mockito.doThrow(new ExpectedRuntimeException()).when(operationPacketHandler).handle(packet);

        transceiver.receive(packet);
    }
}

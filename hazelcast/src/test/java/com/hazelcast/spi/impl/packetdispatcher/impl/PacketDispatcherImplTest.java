package com.hazelcast.spi.impl.packetdispatcher.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
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

import static com.hazelcast.nio.Packet.FLAG_BIND;
import static com.hazelcast.nio.Packet.FLAG_EVENT;
import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PacketDispatcherImplTest extends HazelcastTestSupport {

    private PacketHandler operationExecutor;
    private PacketHandler eventPacketHandler;
    private PacketDispatcherImpl dispatcher;
    private PacketHandler connectionManagerPacketHandler;
    private PacketHandler responseHandler;

    @Before
    public void setup() {
        ILogger logger = Logger.getLogger(getClass());
        operationExecutor = mock(PacketHandler.class);
        responseHandler = mock(PacketHandler.class);
        eventPacketHandler = mock(PacketHandler.class);
        connectionManagerPacketHandler = mock(PacketHandler.class);

        dispatcher = new PacketDispatcherImpl(
                logger,
                operationExecutor,
                responseHandler,
                eventPacketHandler,
                connectionManagerPacketHandler);
    }

    @Test
    public void whenOperationPacket() throws Exception {
        Packet packet = new Packet()
                .setAllFlags(FLAG_OP);

        dispatcher.dispatch(packet);

        verify(operationExecutor).handle(packet);
        verifyZeroInteractions(responseHandler);
        verifyZeroInteractions(eventPacketHandler);
        verifyZeroInteractions(connectionManagerPacketHandler);
    }

    @Test
    public void whenUrgentOperationPacket() throws Exception {
        Packet packet = new Packet()
                .setAllFlags(FLAG_OP | FLAG_URGENT);

        dispatcher.dispatch(packet);

        verify(operationExecutor).handle(packet);
        verifyZeroInteractions(responseHandler);
        verifyZeroInteractions(eventPacketHandler);
        verifyZeroInteractions(connectionManagerPacketHandler);
    }


    @Test
    public void whenOperationResponsePacket() throws Exception {
        Packet packet = new Packet()
                .setAllFlags(FLAG_OP | FLAG_RESPONSE);

        dispatcher.dispatch(packet);

        verify(responseHandler).handle(packet);
        verifyZeroInteractions(operationExecutor);
        verifyZeroInteractions(eventPacketHandler);
        verifyZeroInteractions(connectionManagerPacketHandler);
    }

    @Test
    public void whenUrgentOperationResponsePacket() throws Exception {
        Packet packet = new Packet()
                .setAllFlags(FLAG_OP | FLAG_RESPONSE | FLAG_URGENT);

        dispatcher.dispatch(packet);

        verify(responseHandler).handle(packet);
        verifyZeroInteractions(operationExecutor);
        verifyZeroInteractions(eventPacketHandler);
        verifyZeroInteractions(connectionManagerPacketHandler);
    }


    @Test
    public void whenEventPacket() throws Exception {
        Packet packet = new Packet()
                .setFlag(FLAG_EVENT);

        dispatcher.dispatch(packet);

        verify(eventPacketHandler).handle(packet);
        verifyZeroInteractions(operationExecutor);
        verifyZeroInteractions(connectionManagerPacketHandler);
        verifyZeroInteractions(responseHandler);
    }

    @Test
    public void whenBindPacket() throws Exception {
        Packet packet = new Packet()
                .setFlag(FLAG_BIND);

        dispatcher.dispatch(packet);

        verify(connectionManagerPacketHandler).handle(packet);
        verifyZeroInteractions(operationExecutor);
        verifyZeroInteractions(eventPacketHandler);
        verifyZeroInteractions(responseHandler);

    }

    // unrecognized packets are logged. No handlers is contacted.
    @Test
    public void whenUnrecognizedPacket_thenSwallowed() throws Exception {
        Packet packet = new Packet();

        dispatcher.dispatch(packet);

        verifyZeroInteractions(connectionManagerPacketHandler);
        verifyZeroInteractions(operationExecutor);
        verifyZeroInteractions(eventPacketHandler);
        verifyZeroInteractions(responseHandler);

    }

    // when one of the handlers throws an exception, the exception is logged but not rethrown
    @Test
    public void whenProblemHandlingPacket_thenSwallowed() throws Exception {
        Packet packet = new Packet()
                .setFlag(FLAG_OP);

        Mockito.doThrow(new ExpectedRuntimeException()).when(operationExecutor).handle(packet);

        dispatcher.dispatch(packet);
    }
}

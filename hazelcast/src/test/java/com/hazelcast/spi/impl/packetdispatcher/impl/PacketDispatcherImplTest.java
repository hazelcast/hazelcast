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
import static com.hazelcast.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PacketDispatcherImplTest extends HazelcastTestSupport {

    private PacketHandler operationExecutor;
    private PacketHandler eventService;
    private PacketHandler connectionManager;
    private PacketHandler responseHandler;
    private PacketHandler invocationMonitor;
    private PacketDispatcherImpl dispatcher;

    @Before
    public void setup() {
        ILogger logger = Logger.getLogger(getClass());
        operationExecutor = mock(PacketHandler.class);
        responseHandler = mock(PacketHandler.class);
        eventService = mock(PacketHandler.class);
        connectionManager = mock(PacketHandler.class);
        invocationMonitor = mock(PacketHandler.class);

        dispatcher = new PacketDispatcherImpl(
                logger,
                operationExecutor,
                responseHandler,
                invocationMonitor,
                eventService,
                connectionManager);
    }

    @Test
    public void whenOperationPacket() throws Exception {
        Packet packet = new Packet()
                .setAllFlags(FLAG_OP);

        dispatcher.dispatch(packet);

        verify(operationExecutor).handle(packet);

        verifyZeroInteractions(responseHandler, eventService, connectionManager, invocationMonitor);
    }

    @Test
    public void whenUrgentOperationPacket() throws Exception {
        Packet packet = new Packet()
                .setAllFlags(FLAG_OP | FLAG_URGENT);

        dispatcher.dispatch(packet);

        verify(operationExecutor).handle(packet);

        verifyZeroInteractions(responseHandler, eventService, connectionManager, invocationMonitor);
    }


    @Test
    public void whenOperationResponsePacket() throws Exception {
        Packet packet = new Packet()
                .setAllFlags(FLAG_OP | FLAG_RESPONSE);

        dispatcher.dispatch(packet);

        verify(responseHandler).handle(packet);
        verifyZeroInteractions(operationExecutor, eventService, connectionManager, invocationMonitor);
    }

    @Test
    public void whenUrgentOperationResponsePacket() throws Exception {
        Packet packet = new Packet()
                .setAllFlags(FLAG_OP | FLAG_RESPONSE | FLAG_URGENT);

        dispatcher.dispatch(packet);

        verify(responseHandler).handle(packet);
        verifyZeroInteractions(operationExecutor, eventService, connectionManager, invocationMonitor);
    }


    @Test
    public void whenOperationControlPacket() throws Exception {
        Packet packet = new Packet()
                .setAllFlags(FLAG_OP | FLAG_OP_CONTROL);

        dispatcher.dispatch(packet);

        verify(invocationMonitor).handle(packet);

        verifyZeroInteractions(responseHandler, operationExecutor, eventService, connectionManager);
    }


    @Test
    public void whenEventPacket() throws Exception {
        Packet packet = new Packet()
                .setFlag(FLAG_EVENT);

        dispatcher.dispatch(packet);

        verify(eventService).handle(packet);
        verifyZeroInteractions(responseHandler, operationExecutor, connectionManager, invocationMonitor);
    }

    @Test
    public void whenBindPacket() throws Exception {
        Packet packet = new Packet()
                .setFlag(FLAG_BIND);

        dispatcher.dispatch(packet);

        verify(connectionManager).handle(packet);
        verifyZeroInteractions(responseHandler, operationExecutor, eventService, invocationMonitor);

    }

    // unrecognized packets are logged. No handlers is contacted.
    @Test
    public void whenUnrecognizedPacket_thenSwallowed() throws Exception {
        Packet packet = new Packet();

        dispatcher.dispatch(packet);

        verifyZeroInteractions(responseHandler, operationExecutor, eventService, connectionManager, invocationMonitor);
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

/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import static com.hazelcast.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.nio.Packet.FLAG_OP_RESPONSE;
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
    private PacketHandler jetService;

    @Before
    public void setup() {
        ILogger logger = Logger.getLogger(getClass());
        operationExecutor = mock(PacketHandler.class);
        responseHandler = mock(PacketHandler.class);
        eventService = mock(PacketHandler.class);
        connectionManager = mock(PacketHandler.class);
        invocationMonitor = mock(PacketHandler.class);
        jetService = mock(PacketHandler.class);

        dispatcher = new PacketDispatcherImpl(
                logger,
                operationExecutor,
                responseHandler,
                invocationMonitor,
                eventService,
                connectionManager,
                jetService);
    }

    @Test
    public void whenOperationPacket() throws Exception {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION);

        dispatcher.dispatch(packet);

        verify(operationExecutor).handle(packet);

        verifyZeroInteractions(responseHandler, eventService, connectionManager, invocationMonitor, jetService);
    }

    @Test
    public void whenUrgentOperationPacket() throws Exception {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION).raiseFlags(FLAG_URGENT);

        dispatcher.dispatch(packet);

        verify(operationExecutor).handle(packet);

        verifyZeroInteractions(responseHandler, eventService, connectionManager, invocationMonitor, jetService);
    }


    @Test
    public void whenOperationResponsePacket() throws Exception {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION).raiseFlags(FLAG_OP_RESPONSE);

        dispatcher.dispatch(packet);

        verify(responseHandler).handle(packet);
        verifyZeroInteractions(operationExecutor, eventService, connectionManager, invocationMonitor, jetService);
    }

    @Test
    public void whenUrgentOperationResponsePacket() throws Exception {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION).raiseFlags(FLAG_OP_RESPONSE | FLAG_URGENT);

        dispatcher.dispatch(packet);

        verify(responseHandler).handle(packet);
        verifyZeroInteractions(operationExecutor, eventService, connectionManager, invocationMonitor, jetService);
    }


    @Test
    public void whenOperationControlPacket() throws Exception {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION).raiseFlags(FLAG_OP_CONTROL);

        dispatcher.dispatch(packet);

        verify(invocationMonitor).handle(packet);

        verifyZeroInteractions(responseHandler, operationExecutor, eventService, connectionManager, jetService);
    }


    @Test
    public void whenEventPacket() throws Exception {
        Packet packet = new Packet().setPacketType(Packet.Type.EVENT);

        dispatcher.dispatch(packet);

        verify(eventService).handle(packet);
        verifyZeroInteractions(responseHandler, operationExecutor, connectionManager, invocationMonitor, jetService);
    }

    @Test
    public void whenBindPacket() throws Exception {
        Packet packet = new Packet().setPacketType(Packet.Type.BIND);

        dispatcher.dispatch(packet);

        verify(connectionManager).handle(packet);
        verifyZeroInteractions(responseHandler, operationExecutor, eventService, invocationMonitor, jetService);
    }

    @Test
    public void whenJetPacket() throws Exception {
        Packet packet = new Packet().setPacketType(Packet.Type.JET);
        dispatcher.dispatch(packet);

        verify(jetService).handle(packet);
        verifyZeroInteractions(responseHandler, operationExecutor, connectionManager, eventService, invocationMonitor);
    }

    // unrecognized packets are logged. No handlers is contacted.
    @Test
    public void whenUnrecognizedPacket_thenSwallowed() throws Exception {
        Packet packet = new Packet().setPacketType(Packet.Type.NULL);

        dispatcher.dispatch(packet);

        verifyZeroInteractions(responseHandler, operationExecutor, eventService, connectionManager, invocationMonitor,
                jetService);
    }

    // when one of the handlers throws an exception, the exception is logged but not rethrown
    @Test
    public void whenProblemHandlingPacket_thenSwallowed() throws Exception {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION);

        Mockito.doThrow(new ExpectedRuntimeException()).when(operationExecutor).handle(packet);

        dispatcher.dispatch(packet);
    }
}

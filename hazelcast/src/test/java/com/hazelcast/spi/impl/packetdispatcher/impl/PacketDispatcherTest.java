/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.PacketDispatcher;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.internal.nio.Packet.FLAG_OP_RESPONSE;
import static com.hazelcast.internal.nio.Packet.FLAG_URGENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PacketDispatcherTest extends HazelcastTestSupport {

    private Consumer<Packet> operationExecutor;
    private Consumer<Packet> eventService;
//    private Consumer<Packet> connectionManager;
    private Consumer<Packet> responseHandler;
    private Consumer<Packet> invocationMonitor;
    private PacketDispatcher dispatcher;
    private Consumer<Packet> jetService;

    @Before
    public void setup() {
        ILogger logger = Logger.getLogger(getClass());
        operationExecutor = mock(Consumer.class);
        responseHandler = mock(Consumer.class);
        eventService = mock(Consumer.class);
        invocationMonitor = mock(Consumer.class);
        jetService = mock(Consumer.class);

        dispatcher = new PacketDispatcher(
                logger,
                operationExecutor,
                responseHandler,
                invocationMonitor,
                eventService,
                jetService
        );
    }

    @Test
    public void whenOperationPacket() {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION);

        dispatcher.accept(packet);

        verify(operationExecutor).accept(packet);

        verifyZeroInteractions(responseHandler, eventService, invocationMonitor, jetService);
    }

    @Test
    public void whenUrgentOperationPacket() {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION).raiseFlags(FLAG_URGENT);

        dispatcher.accept(packet);

        verify(operationExecutor).accept(packet);

        verifyZeroInteractions(responseHandler, eventService, invocationMonitor, jetService);
    }


    @Test
    public void whenOperationResponsePacket() {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION).raiseFlags(FLAG_OP_RESPONSE);

        dispatcher.accept(packet);

        verify(responseHandler).accept(packet);
        verifyZeroInteractions(operationExecutor, eventService, invocationMonitor, jetService);
    }

    @Test
    public void whenUrgentOperationResponsePacket() {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION).raiseFlags(FLAG_OP_RESPONSE | FLAG_URGENT);

        dispatcher.accept(packet);

        verify(responseHandler).accept(packet);
        verifyZeroInteractions(operationExecutor, eventService, invocationMonitor, jetService);
    }


    @Test
    public void whenOperationControlPacket() {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION).raiseFlags(FLAG_OP_CONTROL);

        dispatcher.accept(packet);

        verify(invocationMonitor).accept(packet);

        verifyZeroInteractions(responseHandler, operationExecutor, eventService, jetService);
    }


    @Test
    public void whenEventPacket() {
        Packet packet = new Packet().setPacketType(Packet.Type.EVENT);

        dispatcher.accept(packet);

        verify(eventService).accept(packet);
        verifyZeroInteractions(responseHandler, operationExecutor, invocationMonitor, jetService);
    }

    @Test
    public void whenJetPacket() {
        Packet packet = new Packet().setPacketType(Packet.Type.JET);
        dispatcher.accept(packet);

        verify(jetService).accept(packet);
        verifyZeroInteractions(responseHandler, operationExecutor, eventService, invocationMonitor);
    }

    // unrecognized packets are logged. No handlers is contacted.
    @Test
    public void whenUnrecognizedPacket_thenSwallowed() {
        Packet packet = new Packet().setPacketType(Packet.Type.NULL);

        dispatcher.accept(packet);

        verifyZeroInteractions(responseHandler, operationExecutor, eventService, invocationMonitor, jetService);
    }

    // when one of the handlers throws an exception, the exception is logged but not rethrown
    @Test
    public void whenProblemHandlingPacket_thenSwallowed() {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION);

        Mockito.doThrow(new ExpectedRuntimeException()).when(operationExecutor).accept(packet);

        dispatcher.accept(packet);
    }
}

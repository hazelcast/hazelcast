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

package com.hazelcast.nio.tcp;

import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageChannelInboundHandler;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MessageHandlerImplTest {

    private MessageHandlerImpl messageHandler;
    private IOService ioService;
    private Connection connection;
    private SwCounter counter;
    private ClientMessageChannelInboundHandler inboundHandler;
    private ClientEngine clientEngine;

    @Before
    public void setup() throws IOException {
        ioService = mock(IOService.class);
        connection = mock(Connection.class);
        counter = SwCounter.newSwCounter();
        clientEngine = mock(ClientEngine.class);
        messageHandler = new MessageHandlerImpl(connection, clientEngine);
        inboundHandler = new ClientMessageChannelInboundHandler(messageHandler);
        inboundHandler.setNormalPacketsRead(counter);
    }

    @Test
    public void test() throws Exception {
        ClientMessage message = ClientMessage.createForEncode(1000)
                .setPartitionId(10)
                .setMessageType(1)
                .setCorrelationId(1)
                .addFlag(ClientMessage.BEGIN_AND_END_FLAGS);

        ByteBuffer bb = ByteBuffer.allocate(1000);
        message.writeTo(bb);
        bb.flip();

        inboundHandler.onRead(bb);

        verify(clientEngine).handleClientMessage(any(ClientMessage.class), eq(connection));
    }
}

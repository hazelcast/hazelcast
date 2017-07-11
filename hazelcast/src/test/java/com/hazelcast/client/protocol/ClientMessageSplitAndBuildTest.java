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

package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.util.ClientMessageChannelInboundHandler;
import com.hazelcast.client.impl.protocol.util.ClientMessageSplitter;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMessageSplitAndBuildTest {

    private SwCounter readCounter = SwCounter.newSwCounter();

    @Test
    public void splitAndBuild() throws Exception {
        int FRAME_SIZE = 50;
        String s = UUID.randomUUID().toString();
        final ClientMessage expectedClientMessage = ClientAuthenticationCodec.encodeRequest(s, s, s, s, true, s, (byte) 1,
                BuildInfoProvider.getBuildInfo().getVersion());
        expectedClientMessage.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
        List<ClientMessage> subFrames = ClientMessageSplitter.getSubFrames(FRAME_SIZE, expectedClientMessage);
        ClientMessageChannelInboundHandler clientMessageReadHandler = new ClientMessageChannelInboundHandler(
                new ClientMessageChannelInboundHandler.MessageHandler() {
                    @Override
                    public void handleMessage(ClientMessage message) {
                        message.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
                        assertEquals(expectedClientMessage, message);
                    }
                });
        clientMessageReadHandler.setNormalPacketsRead(readCounter);
        for (ClientMessage subFrame : subFrames) {
            clientMessageReadHandler.onRead(ByteBuffer.wrap(subFrame.buffer().byteArray(), 0, subFrame.getFrameLength()));
        }
    }

    @Test
    public void splitTest_checkSize() {
        int FRAME_SIZE = 50;
        String s = UUID.randomUUID().toString();
        ClientMessage clientMessage = ClientAuthenticationCodec.encodeRequest(s, s, s, s, true, s, (byte) 1,
                BuildInfoProvider.getBuildInfo().getVersion());
        clientMessage.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
        List<ClientMessage> subFrames = ClientMessageSplitter.getSubFrames(FRAME_SIZE, clientMessage);

        for (int i = 0; i < subFrames.size() - 1; i++) {
            assertEquals(FRAME_SIZE, subFrames.get(i).getFrameLength());
        }
        assertTrue(subFrames.get(subFrames.size() - 1).getFrameLength() <= FRAME_SIZE);
    }

    @Test
    public void splitAndBuild_multipleMessages() throws Exception {
        int FRAME_SIZE = 50;
        int NUMBER_OF_MESSAGES = 5;

        List<List<ClientMessage>> framedClientMessages = new ArrayList<List<ClientMessage>>(NUMBER_OF_MESSAGES);
        final List<ClientMessage> expectedClientMessages = new ArrayList<ClientMessage>();

        for (int id = 0; id < NUMBER_OF_MESSAGES; id++) {
            String s = UUID.randomUUID().toString();
            ClientMessage expectedClientMessage = ClientAuthenticationCodec.encodeRequest(s, s, s, s, true, s, (byte) 1,
                    BuildInfoProvider.getBuildInfo().getVersion());
            expectedClientMessage.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
            expectedClientMessage.setCorrelationId(id);

            expectedClientMessages.add(expectedClientMessage);
            framedClientMessages.add(ClientMessageSplitter.getSubFrames(FRAME_SIZE, expectedClientMessage));
        }

        ClientMessageChannelInboundHandler clientMessageReadHandler = new ClientMessageChannelInboundHandler(
                new ClientMessageChannelInboundHandler.MessageHandler() {
                    @Override
                    public void handleMessage(ClientMessage message) {
                        int correlationId = (int) message.getCorrelationId();
                        message.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
                        assertEquals(expectedClientMessages.get(correlationId), message);
                    }
                });
        clientMessageReadHandler.setNormalPacketsRead(readCounter);

        int currentFrameIndex[] = new int[NUMBER_OF_MESSAGES];
        for (int nFinishedMessages = 0; nFinishedMessages < NUMBER_OF_MESSAGES; ) {
            for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
                List<ClientMessage> clientMessageFrames = framedClientMessages.get(i);
                if (currentFrameIndex[i] >= clientMessageFrames.size()) {
                    nFinishedMessages++;
                    break;
                }
                ClientMessage subFrame = clientMessageFrames.get(currentFrameIndex[i]);
                clientMessageReadHandler.onRead(ByteBuffer.wrap(subFrame.buffer().byteArray(), 0, subFrame.getFrameLength()));
                currentFrameIndex[i]++;
            }
        }
    }

    @Test
    public void splitAndBuild_whenMessageIsAlreadySmallerThanFrameSize() throws Exception {
        String s = UUID.randomUUID().toString();
        final ClientMessage expectedClientMessage = ClientAuthenticationCodec.encodeRequest(s, s, s, s, true, s, (byte) 1,
                BuildInfoProvider.getBuildInfo().getVersion());
        expectedClientMessage.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
        List<ClientMessage> subFrames = ClientMessageSplitter.getSubFrames(expectedClientMessage.getFrameLength() + 1, expectedClientMessage);
        ClientMessageChannelInboundHandler clientMessageReadHandler = new ClientMessageChannelInboundHandler(
                new ClientMessageChannelInboundHandler.MessageHandler() {
                    @Override
                    public void handleMessage(ClientMessage message) {
                        message.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
                        assertEquals(expectedClientMessage, message);
                    }
                });
        clientMessageReadHandler.setNormalPacketsRead(readCounter);
        for (ClientMessage subFrame : subFrames) {
            clientMessageReadHandler.onRead(ByteBuffer.wrap(subFrame.buffer().byteArray(), 0, subFrame.getFrameLength()));
        }
    }
}

/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.util.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.util.ClientMessageSplitter;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.nio.Connection;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMessageSplitAndBuildTest {

    private SwCounter readCounter;

    @Before
    public void before() {
        readCounter = SwCounter.newSwCounter();
    }

    @Test
    public void splitAndBuild() {
        int FRAME_SIZE = 50;
        String s = UUID.randomUUID().toString();
        final ClientMessage expectedClientMessage = ClientAuthenticationCodec.encodeRequest(s, s, s, s, true, s, (byte) 1,
                BuildInfoProvider.getBuildInfo().getVersion());
        expectedClientMessage.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
        List<ClientMessage> subFrames = ClientMessageSplitter.getSubFrames(FRAME_SIZE, expectedClientMessage);
        ClientMessageDecoder decoder = new ClientMessageDecoder(
                mock(Connection.class),
                new Consumer<ClientMessage>() {
                    @Override
                    public void accept(ClientMessage message) {
                        message.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
                        assertEquals(expectedClientMessage, message);
                    }
                });
        decoder.setNormalPacketsRead(readCounter);

        for (ClientMessage subFrame : subFrames) {
            ByteBuffer src = ByteBuffer.wrap(subFrame.buffer().byteArray(), 0, subFrame.getFrameLength());
            src.flip();
            decoder.src(src);
            decoder.onRead();
        }

        decoder.onRead();
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
    public void splitAndBuild_multipleMessages() {
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

        ClientMessageDecoder decoder = new ClientMessageDecoder(
                mock(Connection.class),
                new Consumer<ClientMessage>() {
                    @Override
                    public void accept(ClientMessage message) {
                        int correlationId = (int) message.getCorrelationId();
                        message.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
                        assertEquals(expectedClientMessages.get(correlationId), message);
                    }
                });
        decoder.setNormalPacketsRead(readCounter);

        int[] currentFrameIndex = new int[NUMBER_OF_MESSAGES];
        for (int nFinishedMessages = 0; nFinishedMessages < NUMBER_OF_MESSAGES; ) {
            for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
                List<ClientMessage> clientMessageFrames = framedClientMessages.get(i);
                if (currentFrameIndex[i] >= clientMessageFrames.size()) {
                    nFinishedMessages++;
                    break;
                }
                ClientMessage subFrame = clientMessageFrames.get(currentFrameIndex[i]);

                ByteBuffer src = ByteBuffer.wrap(subFrame.buffer().byteArray(), 0, subFrame.getFrameLength());
                src.flip();
                decoder.src(src);
                currentFrameIndex[i]++;
            }
        }
    }

    @Test
    public void splitAndBuild_whenMessageIsAlreadySmallerThanFrameSize() {
        String s = UUID.randomUUID().toString();
        final ClientMessage expectedClientMessage = ClientAuthenticationCodec.encodeRequest(s, s, s, s, true, s, (byte) 1,
                BuildInfoProvider.getBuildInfo().getVersion());
        expectedClientMessage.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
        List<ClientMessage> subFrames = ClientMessageSplitter.getSubFrames(
                expectedClientMessage.getFrameLength() + 1, expectedClientMessage);
        ClientMessageDecoder decoder = new ClientMessageDecoder(
                mock(Connection.class),
                new Consumer<ClientMessage>() {
                    @Override
                    public void accept(ClientMessage message) {
                        message.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
                        assertEquals(expectedClientMessage, message);
                    }
                });
        decoder.setNormalPacketsRead(readCounter);
        for (ClientMessage subFrame : subFrames) {
            ByteBuffer src = ByteBuffer.wrap(subFrame.buffer().byteArray(), 0, subFrame.getFrameLength());
            src.flip();
            decoder.src(src);
            decoder.onRead();
        }
    }
}

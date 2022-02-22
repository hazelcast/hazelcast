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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.client.impl.protocol.util.ClientMessageSplitter.getFragments;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.test.HazelcastTestSupport.generateRandomString;
import static groovy.util.GroovyTestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMessageSplitAndBuildTest {

    private ClientMessage clientMessage1;
    private ClientMessage clientMessage2;

    private ClientMessage createMessage() {
        String clusterName = generateRandomString(1000);
        String username = generateRandomString(1000);
        String password = generateRandomString(1000);
        UUID uuid = UuidUtil.newUnsecureUUID();
        String clientType = generateRandomString(1000);
        String clientSerializationVersion = generateRandomString(1000);
        String clientName = generateRandomString(1000);
        LinkedList<String> labels = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            labels.add(generateRandomString(1000));
        }

        return ClientAuthenticationCodec.encodeRequest(clusterName, username, password, uuid, clientType,
                (byte) 1, clientSerializationVersion, clientName, labels);
    }

    @Before
    public void setUp() throws Exception {
        clientMessage1 = createMessage();
        clientMessage2 = createMessage();
    }

    @Test
    public void splitAndBuild() {
        Queue<ClientMessage> outputQueue = new ConcurrentLinkedQueue<>();
        List<ClientMessage> fragments = getFragments(128, clientMessage1);
        for (ClientMessage fragment : fragments) {
            outputQueue.offer(fragment);
        }

        ClientMessageEncoder encoder = new ClientMessageEncoder();
        encoder.src(outputQueue::poll);

        ByteBuffer buffer = ByteBuffer.allocate(100000);
        upcast(buffer).flip();
        encoder.dst(buffer);

        HandlerStatus result = encoder.onWrite();

        Assert.assertEquals(CLEAN, result);

        AtomicReference<ClientMessage> resultingMessage = new AtomicReference<>();
        ClientMessageDecoder decoder = new ClientMessageDecoder(null, resultingMessage::set, null);
        decoder.setNormalPacketsRead(SwCounter.newSwCounter());

        upcast(buffer).position(buffer.limit());

        decoder.src(buffer);
        decoder.onRead();

        assertEquals(getNumberOfFrames(clientMessage1), getNumberOfFrames(resultingMessage.get()));
        assertEquals(clientMessage1.getFrameLength(), resultingMessage.get().getFrameLength());
        assertEquals(0, decoder.builderBySessionIdMap.size());
    }

    @Test
    public void fragmentFieldAccessTest() {
        List<ClientMessage> fragments = getFragments(128, clientMessage1);
        ClientMessage firstMessage = fragments.get(0);
        ClientMessage.ForwardFrameIterator forwardFrameIterator = firstMessage.frameIterator();
        // skip the first frame as it is the fragmentation frame
        forwardFrameIterator.next();
        byte[] startFrameContent = forwardFrameIterator.next().content;
        assertEquals(clientMessage1.getMessageType(), Bits.readIntL(startFrameContent, ClientMessage.TYPE_FIELD_OFFSET));
        assertEquals(clientMessage1.getCorrelationId(),
                Bits.readIntL(startFrameContent, ClientMessage.CORRELATION_ID_FIELD_OFFSET));
    }

    @Test
    public void splitAndBuild_multipleMessages() {
        Queue<ClientMessage> outputQueue = new ConcurrentLinkedQueue<>();
        List<ClientMessage> fragments1 = getFragments(128, clientMessage1);
        List<ClientMessage> fragments2 = getFragments(128, clientMessage2);
        Iterator<ClientMessage> iterator = fragments2.iterator();
        for (ClientMessage fragment : fragments1) {
            outputQueue.offer(fragment);
            outputQueue.offer(iterator.next());
        }

        ClientMessageEncoder encoder = new ClientMessageEncoder();
        encoder.src(outputQueue::poll);

        ByteBuffer buffer = ByteBuffer.allocate(100000);
        upcast(buffer).flip();
        encoder.dst(buffer);

        HandlerStatus result = encoder.onWrite();

        Assert.assertEquals(CLEAN, result);

        Queue<ClientMessage> inputQueue = new ConcurrentLinkedQueue<>();
        ClientMessageDecoder decoder = new ClientMessageDecoder(null, inputQueue::offer, null);
        decoder.setNormalPacketsRead(SwCounter.newSwCounter());

        upcast(buffer).position(buffer.limit());

        decoder.src(buffer);
        decoder.onRead();

        ClientMessage actualMessage1 = inputQueue.poll();
        ClientMessage actualMessage2 = inputQueue.poll();

        assertMessageEquals(clientMessage1, actualMessage1);
        assertMessageEquals(clientMessage2, actualMessage2);
        assertEquals(0, decoder.builderBySessionIdMap.size());
    }

    @Test
    public void splitAndBuild_whenMessageIsAlreadySmallerThanFrameSize() {
        Queue<ClientMessage> outputQueue = new ConcurrentLinkedQueue<>();
        List<ClientMessage> fragments = getFragments(100000, clientMessage1);
        for (ClientMessage fragment : fragments) {
            outputQueue.offer(fragment);
        }

        ClientMessageEncoder encoder = new ClientMessageEncoder();
        encoder.src(outputQueue::poll);

        ByteBuffer buffer = ByteBuffer.allocate(100000);
        upcast(buffer).flip();
        encoder.dst(buffer);

        HandlerStatus result = encoder.onWrite();

        Assert.assertEquals(CLEAN, result);

        AtomicReference<ClientMessage> resultingMessage = new AtomicReference<>();
        ClientMessageDecoder decoder = new ClientMessageDecoder(null, resultingMessage::set, null);
        decoder.setNormalPacketsRead(SwCounter.newSwCounter());

        upcast(buffer).position(buffer.limit());

        decoder.src(buffer);
        decoder.onRead();

        assertEquals(clientMessage1.getFrameLength(), resultingMessage.get().getFrameLength());
        assertEquals(0, decoder.builderBySessionIdMap.size());
    }

    private void assertMessageEquals(ClientMessage expected, ClientMessage actual) {
        //these flags related to framing and can differ between two semantically equal messages
        int mask = ~(ClientMessage.UNFRAGMENTED_MESSAGE | ClientMessage.IS_FINAL_FLAG);

        ClientMessage.ForwardFrameIterator actualIterator = actual.frameIterator();
        ClientMessage.ForwardFrameIterator expectedFrameIterator = expected.frameIterator();
        while (expectedFrameIterator.hasNext()) {
            ClientMessage.Frame actualFrame = actualIterator.next();
            ClientMessage.Frame expectedFrame = expectedFrameIterator.next();
            assertEquals(actualFrame.getSize(), expectedFrame.getSize());
            assertEquals(actualFrame.flags & mask, expectedFrame.flags & mask);
            Assert.assertArrayEquals(expectedFrame.content, actualFrame.content);
        }
    }

    private int getNumberOfFrames(ClientMessage message) {
        int size = 0;
        ClientMessage.ForwardFrameIterator frameIterator = message.frameIterator();
        while (frameIterator.hasNext()) {
            frameIterator.next();
            ++size;
        }
        return size;
    }
}

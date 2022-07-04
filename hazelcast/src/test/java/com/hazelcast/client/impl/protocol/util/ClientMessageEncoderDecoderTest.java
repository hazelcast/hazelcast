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
import com.hazelcast.client.impl.protocol.ClientMessage.Frame;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutCodec;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.client.impl.protocol.ClientMessage.IS_FINAL_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;
import static com.hazelcast.client.impl.protocol.util.ClientMessageSplitter.getFragments;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMessageEncoderDecoderTest extends HazelcastTestSupport {

    @Test
    public void test() {
        ClientMessage message = ClientMessage.createForEncode();
        message.add(new Frame(new byte[100], UNFRAGMENTED_MESSAGE | IS_FINAL_FLAG));
        message.setMessageType(MapPutCodec.REQUEST_MESSAGE_TYPE);
        AtomicReference<ClientMessage> reference = new AtomicReference<>(message);


        ClientMessageEncoder encoder = new ClientMessageEncoder();
        encoder.src(() -> reference.getAndSet(null));

        ByteBuffer buffer = ByteBuffer.allocate(1000);
        upcast(buffer).flip();
        encoder.dst(buffer);

        HandlerStatus result = encoder.onWrite();

        assertEquals(CLEAN, result);

        AtomicReference<ClientMessage> resultingMessage = new AtomicReference<>();
        ClientMessageDecoder decoder = new ClientMessageDecoder(null, resultingMessage::set, null);
        decoder.setNormalPacketsRead(SwCounter.newSwCounter());

        upcast(buffer).position(buffer.limit());

        decoder.src(buffer);
        decoder.onRead();

        assertEquals(message.getMessageType(), resultingMessage.get().getMessageType());
        assertEquals(message.getFrameLength(), resultingMessage.get().getFrameLength());
        assertEquals(message.getHeaderFlags(), resultingMessage.get().getHeaderFlags());
        assertEquals(message.getPartitionId(), resultingMessage.get().getPartitionId());
    }

    @Test
    public void testPut() {
        ClientMessage message =
                MapPutCodec.encodeRequest("map", new HeapData(new byte[100]), new HeapData(new byte[100]), 5, 10);
        AtomicReference<ClientMessage> reference = new AtomicReference<>(message);


        ClientMessageEncoder encoder = new ClientMessageEncoder();
        encoder.src(() -> reference.getAndSet(null));

        ByteBuffer buffer = ByteBuffer.allocate(1000);
        upcast(buffer).flip();
        encoder.dst(buffer);

        HandlerStatus result = encoder.onWrite();

        assertEquals(CLEAN, result);

        AtomicReference<ClientMessage> resultingMessage = new AtomicReference<>();
        ClientMessageDecoder decoder = new ClientMessageDecoder(null, resultingMessage::set, null);
        decoder.setNormalPacketsRead(SwCounter.newSwCounter());

        upcast(buffer).position(buffer.limit());

        decoder.src(buffer);
        decoder.onRead();

        assertEquals(message.getMessageType(), resultingMessage.get().getMessageType());
        assertEquals(message.getFrameLength(), resultingMessage.get().getFrameLength());
        assertEquals(message.getHeaderFlags(), resultingMessage.get().getHeaderFlags());
        assertEquals(message.getPartitionId(), resultingMessage.get().getPartitionId());

        MapPutCodec.RequestParameters parameters = MapPutCodec.decodeRequest(resultingMessage.get());

        assertEquals(5, parameters.threadId);
        assertEquals("map", parameters.name);
    }

    @Test
    public void testAuthenticationRequest() {
        Collection<String> labels = new LinkedList<>();
        labels.add("Label");
        UUID uuid = UUID.randomUUID();
        ClientMessage message = ClientAuthenticationCodec.encodeRequest("cluster", "user", "pass",
                uuid, "JAVA", (byte) 1,
                "1.0", "name", labels);
        AtomicReference<ClientMessage> reference = new AtomicReference<>(message);


        ClientMessageEncoder encoder = new ClientMessageEncoder();
        encoder.src(() -> reference.getAndSet(null));

        ByteBuffer buffer = ByteBuffer.allocate(1000);
        upcast(buffer).flip();
        encoder.dst(buffer);

        HandlerStatus result = encoder.onWrite();

        assertEquals(CLEAN, result);

        AtomicReference<ClientMessage> resultingMessage = new AtomicReference<>();
        ClientMessageDecoder decoder = new ClientMessageDecoder(null, resultingMessage::set, null);
        decoder.setNormalPacketsRead(SwCounter.newSwCounter());

        upcast(buffer).position(buffer.limit());

        decoder.src(buffer);
        decoder.onRead();

        assertEquals(message.getMessageType(), resultingMessage.get().getMessageType());
        assertEquals(message.getFrameLength(), resultingMessage.get().getFrameLength());
        assertEquals(message.getHeaderFlags(), resultingMessage.get().getHeaderFlags());
        assertEquals(message.getPartitionId(), resultingMessage.get().getPartitionId());

        ClientAuthenticationCodec.RequestParameters parameters = ClientAuthenticationCodec.decodeRequest(resultingMessage.get());

        assertEquals("cluster", parameters.clusterName);
        assertEquals("user", parameters.username);
        assertEquals("pass", parameters.password);
        assertEquals(uuid, parameters.uuid);
        assertEquals("JAVA", parameters.clientType);
        assertEquals((byte) 1, parameters.serializationVersion);
        assertEquals("1.0", parameters.clientHazelcastVersion);
        assertEquals("name", parameters.clientName);
        assertArrayEquals(labels.toArray(), parameters.labels.toArray());
    }

    @Test
    public void testAuthenticationResponse() throws UnknownHostException {
        Collection<Member> members = new LinkedList<>();
        Address address1 = new Address("127.0.0.1", 5702);
        members.add(new MemberImpl(address1, MemberVersion.of("3.12"), false, UUID.randomUUID()));
        Address address2 = new Address("127.0.0.1", 5703);
        members.add(new MemberImpl(address2, MemberVersion.of("3.12"), false, UUID.randomUUID()));
        UUID uuid = UUID.randomUUID();
        UUID clusterId = UUID.randomUUID();

        ClientMessage message = ClientAuthenticationCodec.encodeResponse((byte) 2, new Address("127.0.0.1", 5701),
                uuid, (byte) 1, "3.12", 271, clusterId, true);
        AtomicReference<ClientMessage> reference = new AtomicReference<>(message);


        ClientMessageEncoder encoder = new ClientMessageEncoder();
        encoder.src(() -> reference.getAndSet(null));

        ByteBuffer buffer = ByteBuffer.allocate(1000);
        upcast(buffer).flip();
        encoder.dst(buffer);

        HandlerStatus result = encoder.onWrite();

        assertEquals(CLEAN, result);

        AtomicReference<ClientMessage> resultingMessage = new AtomicReference<>();
        ClientMessageDecoder decoder = new ClientMessageDecoder(null, resultingMessage::set, null);
        decoder.setNormalPacketsRead(SwCounter.newSwCounter());

        upcast(buffer).position(buffer.limit());

        decoder.src(buffer);
        decoder.onRead();

        assertEquals(message.getMessageType(), resultingMessage.get().getMessageType());
        assertEquals(message.getFrameLength(), resultingMessage.get().getFrameLength());
        assertEquals(message.getHeaderFlags(), resultingMessage.get().getHeaderFlags());
        assertEquals(message.getPartitionId(), resultingMessage.get().getPartitionId());

        ClientAuthenticationCodec.ResponseParameters parameters = ClientAuthenticationCodec.decodeResponse(resultingMessage.get());

        assertEquals(2, parameters.status);
        assertEquals(new Address("127.0.0.1", 5701), parameters.address);
        assertEquals(uuid, parameters.memberUuid);
        assertEquals(1, parameters.serializationVersion);
        assertEquals("3.12", parameters.serverHazelcastVersion);
        assertEquals(271, parameters.partitionCount);
        assertEquals(clusterId, parameters.clusterId);
        assertEquals(true, parameters.failoverSupported);
    }

    class EventHandler extends MapAddEntryListenerCodec.AbstractEventHandler {

        Data key;
        Data value;
        int eventType;
        UUID uuid;
        int numberOfAffectedEntries;

        @Override
        public void handleEntryEvent(@Nullable Data key, @Nullable Data value, @Nullable Data oldValue,
                                     @Nullable Data mergingValue, int eventType, UUID uuid, int numberOfAffectedEntries) {
            this.key = key;
            this.value = value;
            this.eventType = eventType;
            this.uuid = uuid;
            this.numberOfAffectedEntries = numberOfAffectedEntries;
        }
    }

    @Test
    public void testEvent() {
        HeapData keyData = randomData();
        HeapData valueData = randomData();
        UUID uuid = UUID.randomUUID();
        ClientMessage message = MapAddEntryListenerCodec.encodeEntryEvent(keyData, valueData, null, null,
                1, uuid, 1);
        AtomicReference<ClientMessage> reference = new AtomicReference<>(message);

        ClientMessageEncoder encoder = new ClientMessageEncoder();
        encoder.src(() -> reference.getAndSet(null));

        ByteBuffer buffer = ByteBuffer.allocate(1000);
        upcast(buffer).flip();
        encoder.dst(buffer);

        HandlerStatus result = encoder.onWrite();

        assertEquals(CLEAN, result);

        AtomicReference<ClientMessage> resultingMessage = new AtomicReference<>();
        ClientMessageDecoder decoder = new ClientMessageDecoder(null, resultingMessage::set, null);
        decoder.setNormalPacketsRead(SwCounter.newSwCounter());

        upcast(buffer).position(buffer.limit());

        decoder.src(buffer);
        decoder.onRead();

        assertEquals(message.getMessageType(), resultingMessage.get().getMessageType());
        assertEquals(message.getFrameLength(), resultingMessage.get().getFrameLength());
        assertEquals(message.getHeaderFlags(), resultingMessage.get().getHeaderFlags());
        assertEquals(message.getPartitionId(), resultingMessage.get().getPartitionId());

        EventHandler eventHandler = new EventHandler();
        eventHandler.handle(resultingMessage.get());

        assertEquals(keyData, eventHandler.key);
        assertEquals(valueData, eventHandler.value);
        assertEquals(1, eventHandler.eventType);
        assertEquals(uuid, eventHandler.uuid);
        assertEquals(1, eventHandler.numberOfAffectedEntries);
    }

    @Test
    public void testFragmentedMessageHandling() {
        ClientMessage message = createMessage(10, 9);
        List<ClientMessage> fragments = getFragments(48, message);

        assertEquals(3, fragments.size());

        AtomicReference<Iterator<ClientMessage>> reference = new AtomicReference<>(fragments.iterator());

        ClientMessageEncoder encoder = new ClientMessageEncoder();
        encoder.src(() -> {
            Iterator<ClientMessage> iterator = reference.get();
            if (iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        });

        ByteBuffer buffer = ByteBuffer.allocate(200);
        upcast(buffer).flip();
        encoder.dst(buffer);

        HandlerStatus result = encoder.onWrite();

        assertEquals(CLEAN, result);

        AtomicReference<ClientMessage> resultingMessageRef = new AtomicReference<>();
        ClientMessageDecoder decoder = new ClientMessageDecoder(null, resultingMessageRef::set, null);
        decoder.setNormalPacketsRead(SwCounter.newSwCounter());

        upcast(buffer).position(buffer.limit());

        decoder.src(buffer);
        decoder.onRead();

        ClientMessage resultingMessage = resultingMessageRef.get();

        assertEquals(message.getFrameLength(), resultingMessage.getFrameLength());

        ClientMessage.ForwardFrameIterator expectedIterator = message.frameIterator();
        ClientMessage.ForwardFrameIterator resultingIterator = resultingMessage.frameIterator();
        while (expectedIterator.hasNext()) {
            assertArrayEquals(expectedIterator.next().content, resultingIterator.next().content);
        }
    }

    private ClientMessage createMessage(int frameLength, int frameCount) {
        ClientMessage message = ClientMessage.createForEncode();

        Random random = new Random();
        for (int i = 0; i < frameCount; i++) {
            byte[] content = new byte[frameLength];
            random.nextBytes(content);
            message.add(new Frame(content));
        }
        return message;
    }

    private HeapData randomData() {
        Random random = new Random();
        byte[] key = new byte[100];
        random.nextBytes(key);
        return new HeapData(key);
    }
}

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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PacketDecoderTest extends HazelcastTestSupport {

    private ConsumerStub dispatcher;
    private PacketDecoder decoder;
    private InternalSerializationService serializationService;
    private Counter normalPacketCounter = SwCounter.newSwCounter();
    private Counter priorityPacketCounter = SwCounter.newSwCounter();

    @Before
    public void setup() throws Exception {
        TcpServerConnection connection = mock(TcpServerConnection.class);

        dispatcher = new ConsumerStub();
        decoder = new PacketDecoder(connection, dispatcher);
        decoder.setNormalPacketsRead(normalPacketCounter);
        decoder.setPriorityPacketsRead(priorityPacketCounter);

        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void whenPriorityPacket() throws Exception {
        ByteBuffer src = ByteBuffer.allocate(1000);
        Packet packet = new Packet(serializationService.toBytes("foobar"))
                .raiseFlags(Packet.FLAG_URGENT);
        new PacketIOHelper().writeTo(packet, src);

        decoder.src(src);
        decoder.onRead();

        assertEquals(1, dispatcher.packets.size());
        Packet found = dispatcher.packets.get(0);
        assertEquals(packet, found);
        assertEquals(0, normalPacketCounter.get());
        assertEquals(1, priorityPacketCounter.get());
    }

    @Test
    public void whenNormalPacket() throws Exception {
        ByteBuffer src = ByteBuffer.allocate(1000);
        Packet packet = new Packet(serializationService.toBytes("foobar"));
        new PacketIOHelper().writeTo(packet, src);

        decoder.src(src);
        decoder.onRead();

        assertEquals(1, dispatcher.packets.size());
        Packet found = dispatcher.packets.get(0);
        assertEquals(packet, found);
        assertEquals(1, normalPacketCounter.get());
        assertEquals(0, priorityPacketCounter.get());
    }

    @Test
    public void whenMultiplePackets() throws Exception {
        ByteBuffer src = ByteBuffer.allocate(1000);

        Packet packet1 = new Packet(serializationService.toBytes("packet1"));
        new PacketIOHelper().writeTo(packet1, src);

        Packet packet2 = new Packet(serializationService.toBytes("packet2"));
        new PacketIOHelper().writeTo(packet2, src);

        Packet packet3 = new Packet(serializationService.toBytes("packet3"));
        new PacketIOHelper().writeTo(packet3, src);

        Packet packet4 = new Packet(serializationService.toBytes("packet4"));
        packet4.raiseFlags(Packet.FLAG_URGENT);
        new PacketIOHelper().writeTo(packet4, src);

        decoder.src(src);
        decoder.onRead();

        assertEquals(asList(packet1, packet2, packet3, packet4), dispatcher.packets);
        assertEquals(3, normalPacketCounter.get());
        assertEquals(1, priorityPacketCounter.get());
    }

    class ConsumerStub implements Consumer<Packet> {
        private List<Packet> packets = new LinkedList<Packet>();

        @Override
        public void accept(Packet packet) {
            packets.add(packet);
        }
    }
}

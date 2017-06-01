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

import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.internal.networking.nio.NioChannelReader;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@Ignore
public class MemberChannelInboundHandlerTest extends TcpIpConnection_AbstractTest {

    private MockPacketDispatcher dispatcher;
    private MemberChannelInboundHandler readHandler;
    private long oldPriorityPacketsRead;
    private long oldNormalPacketsRead;
    private NioChannelReader channelReader;

    @Before
    public void setup() throws Exception {
        super.setup();

        connManagerA.start();
        connManagerB.start();

        // currently the tcpIpConnection relies heavily on tcpipconnectionmanager/io-service. So mocking is nightmare.
        // we we create a real connection.
        TcpIpConnection connection = connect(connManagerA, addressB);

        dispatcher = new MockPacketDispatcher();
        readHandler = new MemberChannelInboundHandler(connection, dispatcher);

        channelReader = ((NioChannel) connection.getChannel()).getReader();
        oldNormalPacketsRead = channelReader.getNormalFramesReadCounter().get();
        oldPriorityPacketsRead = channelReader.getPriorityFramesReadCounter().get();
    }

    @Test
    public void whenPriorityPacket() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        Packet packet = new Packet(serializationService.toBytes("foobar"));
        packet.raiseFlags(Packet.FLAG_URGENT);
        packet.writeTo(buffer);

        buffer.flip();
        readHandler.onRead(buffer);

        assertEquals(1, dispatcher.packets.size());
        Packet found = dispatcher.packets.get(0);
        assertEquals(packet, found);
        assertEquals(oldNormalPacketsRead, channelReader.getNormalFramesReadCounter().get());
        assertEquals(oldPriorityPacketsRead + 1, channelReader.getPriorityFramesReadCounter().get());
    }

    @Test
    public void whenNormalPacket() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        Packet packet = new Packet(serializationService.toBytes("foobar"));
        packet.writeTo(buffer);

        buffer.flip();
        readHandler.onRead(buffer);

        assertEquals(1, dispatcher.packets.size());
        Packet found = dispatcher.packets.get(0);
        assertEquals(packet, found);
        assertEquals(oldNormalPacketsRead + 1, channelReader.getNormalFramesReadCounter().get());
        assertEquals(oldPriorityPacketsRead, channelReader.getPriorityFramesReadCounter().get());
    }

    @Test
    public void whenMultiplePackets() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1000);

        Packet packet1 = new Packet(serializationService.toBytes("packet1"));
        packet1.writeTo(buffer);

        Packet packet2 = new Packet(serializationService.toBytes("packet2"));
        packet2.writeTo(buffer);

        Packet packet3 = new Packet(serializationService.toBytes("packet3"));
        packet3.writeTo(buffer);

        Packet packet4 = new Packet(serializationService.toBytes("packet4"));
        packet4.raiseFlags(Packet.FLAG_URGENT);
        packet4.writeTo(buffer);

        buffer.flip();
        readHandler.onRead(buffer);

        assertEquals(asList(packet1, packet2, packet3, packet4), dispatcher.packets);
        assertEquals(oldNormalPacketsRead + 3, channelReader.getNormalFramesReadCounter().get());
        assertEquals(oldPriorityPacketsRead + 1, channelReader.getPriorityFramesReadCounter().get());
    }

    class MockPacketDispatcher implements PacketHandler {
        private List<Packet> packets = new LinkedList<Packet>();

        @Override
        public void handle(Packet packet) throws Exception {
            packets.add(packet);
        }
    }
}

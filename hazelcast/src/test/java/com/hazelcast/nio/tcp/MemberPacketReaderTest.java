package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemberPacketReaderTest extends TcpIpConnection_AbstractTest {

    private MockPacketDispatcher dispatcher;
    private MemberPacketReader reader;
    private long oldPriorityPacketsRead;
    private long oldNormalPacketsRead;
    private ReadHandler readHandler;

    @Before
    public void setup() throws Exception {
        super.setup();

        connManagerA.start();
        connManagerB.start();

        // currently the tcpIpConnection relies heavily on tcpipconnectionmanager/io-service. So mocking is nightmare.
        // we we create a real connection.
        TcpIpConnection connection = connect(connManagerA, addressB);

        dispatcher = new MockPacketDispatcher();
        reader = new MemberPacketReader(connection, dispatcher);

        readHandler = connection.getReadHandler();
        oldNormalPacketsRead = readHandler.getNormalPacketsReadCounter().get();
        oldPriorityPacketsRead = readHandler.getPriorityPacketsReadCounter().get();
    }

    @Test
    public void whenPriorityPacket() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        Packet packet = new Packet(serializationService.toBytes("foobar"));
        packet.setHeader(Packet.HEADER_URGENT);
        packet.writeTo(buffer);

        buffer.flip();
        reader.read(buffer);

        assertEquals(1, dispatcher.packets.size());
        Packet found = dispatcher.packets.get(0);
        assertEquals(packet, found);
        assertEquals(oldNormalPacketsRead, readHandler.getNormalPacketsReadCounter().get());
        assertEquals(oldPriorityPacketsRead + 1, readHandler.getPriorityPacketsReadCounter().get());
    }

    @Test
    public void whenNormalPacket() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        Packet packet = new Packet(serializationService.toBytes("foobar"));
        packet.writeTo(buffer);

        buffer.flip();
        reader.read(buffer);

        assertEquals(1, dispatcher.packets.size());
        Packet found = dispatcher.packets.get(0);
        assertEquals(packet, found);
        assertEquals(oldNormalPacketsRead + 1, readHandler.getNormalPacketsReadCounter().get());
        assertEquals(oldPriorityPacketsRead, readHandler.getPriorityPacketsReadCounter().get());
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
        packet4.setHeader(Packet.HEADER_URGENT);
        packet4.writeTo(buffer);

        buffer.flip();
        reader.read(buffer);

        assertEquals(4, dispatcher.packets.size());
        assertEquals(packet1, dispatcher.packets.get(0));
        assertEquals(packet2, dispatcher.packets.get(1));
        assertEquals(packet3, dispatcher.packets.get(2));
        assertEquals(packet4, dispatcher.packets.get(3));
        assertEquals(oldNormalPacketsRead + 3, readHandler.getNormalPacketsReadCounter().get());
        assertEquals(oldPriorityPacketsRead + 1, readHandler.getPriorityPacketsReadCounter().get());
    }

    class MockPacketDispatcher implements PacketDispatcher {
        private List<Packet> packets = new LinkedList<Packet>();

        @Override
        public void dispatch(Packet packet) {
            packets.add(packet);
        }
    }
}

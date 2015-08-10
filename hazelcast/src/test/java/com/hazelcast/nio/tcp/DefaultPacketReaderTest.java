package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.packettransceiver.PacketTransceiver;
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
public class DefaultPacketReaderTest extends TcpIpConnection_AbstractTest {

    private MockPacketTransceiver packetTransceiver;
    private DefaultPacketReader reader;
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

        packetTransceiver = new MockPacketTransceiver();
        reader = new DefaultPacketReader(connection, packetTransceiver);

        readHandler = connection.getReadHandler();
        oldNormalPacketsRead = readHandler.getNormalPacketsRead().get();
        oldPriorityPacketsRead = readHandler.getPriorityPacketsRead().get();
    }

    @Test
    public void whenPriorityPacket() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        Packet packet = new Packet(serializationService.toBytes("foobar"));
        packet.setHeader(Packet.HEADER_URGENT);
        packet.writeTo(buffer);

        buffer.flip();
        reader.readPacket(buffer);

        assertEquals(1, packetTransceiver.packets.size());
        Packet found = packetTransceiver.packets.get(0);
        assertEquals(packet, found);
        assertEquals(oldNormalPacketsRead, readHandler.getNormalPacketsRead().get());
        assertEquals(oldPriorityPacketsRead + 1, readHandler.getPriorityPacketsRead().get());
    }

    @Test
    public void whenNormalPacket() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        Packet packet = new Packet(serializationService.toBytes("foobar"));
        packet.writeTo(buffer);

        buffer.flip();
        reader.readPacket(buffer);

        assertEquals(1, packetTransceiver.packets.size());
        Packet found = packetTransceiver.packets.get(0);
        assertEquals(packet, found);
        assertEquals(oldNormalPacketsRead + 1, readHandler.getNormalPacketsRead().get());
        assertEquals(oldPriorityPacketsRead, readHandler.getPriorityPacketsRead().get());
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
        reader.readPacket(buffer);

        assertEquals(4, packetTransceiver.packets.size());
        assertEquals(packet1, packetTransceiver.packets.get(0));
        assertEquals(packet2, packetTransceiver.packets.get(1));
        assertEquals(packet3, packetTransceiver.packets.get(2));
        assertEquals(packet4, packetTransceiver.packets.get(3));
        assertEquals(oldNormalPacketsRead + 3, readHandler.getNormalPacketsRead().get());
        assertEquals(oldPriorityPacketsRead+1, readHandler.getPriorityPacketsRead().get());
    }

    class MockPacketTransceiver implements PacketTransceiver {
        private List<Packet> packets = new LinkedList<Packet>();

        @Override
        public boolean transmit(Packet packet, Connection connection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean transmit(Packet packet, Address target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void receive(Packet packet) {
            packets.add(packet);
        }
    }
}

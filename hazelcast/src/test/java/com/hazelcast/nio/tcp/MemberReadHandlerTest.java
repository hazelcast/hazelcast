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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemberReadHandlerTest extends TcpIpConnection_AbstractTest {

    private MockPacketDispatcher dispatcher;
    private MemberReadHandler readHandler;
    private long oldPriorityPacketsRead;
    private long oldNormalPacketsRead;
    private SocketReader socketReader;

    @Before
    public void setup() throws Exception {
        super.setup();

        connManagerA.start();
        connManagerB.start();

        // currently the tcpIpConnection relies heavily on tcpipconnectionmanager/io-service. So mocking is nightmare.
        // we we create a real connection.
        TcpIpConnection connection = connect(connManagerA, addressB);

        dispatcher = new MockPacketDispatcher();
        readHandler = new MemberReadHandler(connection, dispatcher);

        socketReader = connection.getSocketReader();
        oldNormalPacketsRead = socketReader.getNormalFramesReadCounter().get();
        oldPriorityPacketsRead = socketReader.getPriorityFramesReadCounter().get();
    }

    @Test
    public void whenPriorityPacket() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        Packet packet = new Packet(serializationService.toBytes("foobar"));
        packet.setFlag(Packet.FLAG_URGENT);
        packet.writeTo(buffer);

        buffer.flip();
        readHandler.onRead(buffer);

        assertEquals(1, dispatcher.packets.size());
        Packet found = dispatcher.packets.get(0);
        assertEquals(packet, found);
        assertEquals(oldNormalPacketsRead, socketReader.getNormalFramesReadCounter().get());
        assertEquals(oldPriorityPacketsRead + 1, socketReader.getPriorityFramesReadCounter().get());
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
        assertEquals(oldNormalPacketsRead + 1, socketReader.getNormalFramesReadCounter().get());
        assertEquals(oldPriorityPacketsRead, socketReader.getPriorityFramesReadCounter().get());
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
        packet4.setFlag(Packet.FLAG_URGENT);
        packet4.writeTo(buffer);

        buffer.flip();
        readHandler.onRead(buffer);

        assertEquals(asList(packet1, packet2, packet3, packet4), dispatcher.packets);
        assertEquals(oldNormalPacketsRead + 3, socketReader.getNormalFramesReadCounter().get());
        assertEquals(oldPriorityPacketsRead + 1, socketReader.getPriorityFramesReadCounter().get());
    }

    class MockPacketDispatcher implements PacketDispatcher {
        private List<Packet> packets = new LinkedList<Packet>();

        @Override
        public void dispatch(Packet packet) {
            packets.add(packet);
        }
    }
}

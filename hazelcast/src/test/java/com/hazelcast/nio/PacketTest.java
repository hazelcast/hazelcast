package com.hazelcast.nio;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.nio.serialization.PortableContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PacketTest extends HazelcastTestSupport {

    private PortableContext portableContext;

    @Before
    public void before() {
        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        portableContext = node.getSerializationService().getPortableContext();
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testNullData() {
        ByteBuffer bb = ByteBuffer.allocate(100);
        Packet packet = new Packet(null, portableContext);
        packet.writeTo(bb);
        bb.flip();

        Packet loaded = new Packet(portableContext);
        loaded.readFrom(bb);
        assertNull(loaded.getData());
    }

    @Test
    public void testOperationResponse() {
        Packet packet = new Packet(new DefaultData(0, new byte[]{}), portableContext);

        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);
        packet.setResponseCallId(10);
        packet.setResponseBackupCount((byte) 2);

        ByteBuffer bb = ByteBuffer.allocate(100);
        packet.writeTo(bb);

        bb.flip();
        Packet copy = new Packet(new DefaultData(0, new byte[]{}), portableContext);
        copy.readFrom(bb);

        assertTrue(copy.isHeaderSet(Packet.HEADER_OP));
        assertTrue(copy.isHeaderSet(Packet.HEADER_RESPONSE));
        assertEquals("callid not equals", packet.getResponseCallId(), copy.getResponseCallId());
        assertEquals("backupCount not equal", packet.getResponseBackupCount(), copy.getResponseBackupCount());
    }

    // check if the packet can deal with a buffer that is very small, but the data is very large.
    // This means that repeated calls to packet.write/packet.read are needed.
    @Test
    public void largeValue() {
        DefaultData originalData = new DefaultData(1, generateRandomString(100000).getBytes());
        Packet originalPacket = new Packet(originalData, mock(PortableContext.class));

        Packet clonedPacket = new Packet(mock(PortableContext.class));

        ByteBuffer bb = ByteBuffer.allocate(20);
        boolean writeCompleted;
        boolean readCompleted;
        do {
            writeCompleted = originalPacket.writeTo(bb);
            bb.flip();
            readCompleted = clonedPacket.readFrom(bb);
            bb.clear();
        } while (!writeCompleted);

        assertTrue(readCompleted);

        assertEquals(originalPacket.getHeader(), clonedPacket.getHeader());
        assertEquals(originalPacket.getData(), clonedPacket.getData());
    }

    @Test
    public void lotsOfPackets() {
        List<Packet> originalPackets = new LinkedList<Packet>();
        Random random = new Random();
        for (int k = 0; k < 1000; k++) {
            byte[] bytes = generateRandomString(random.nextInt(1000)).getBytes();
            DefaultData originalData = new DefaultData(1, bytes);
            Packet originalPacket = new Packet(originalData, mock(PortableContext.class));
            originalPackets.add(originalPacket);
        }

        ByteBuffer bb = ByteBuffer.allocate(20);

        int k = 0;
        for (Packet originalPacket : originalPackets) {
            Packet clonedPacket = new Packet(mock(PortableContext.class));
            boolean writeCompleted;
            boolean readCompleted;
            do {
                writeCompleted = originalPacket.writeTo(bb);
                bb.flip();
                readCompleted = clonedPacket.readFrom(bb);
                bb.clear();
            } while (!writeCompleted);

            assertTrue(readCompleted);
            assertEquals("failed at:" + k, originalPacket.getHeader(), clonedPacket.getHeader());
            assertEquals("failed at:" + k, originalPacket.getData(), clonedPacket.getData());
            k++;
        }
    }

    // This test verifies that writing a Packet to a ByteBuffer and then reading it from the ByteBuffer, gives the
    // same Packet (content).
    @Test
    public void cloningOfPacket() {
        DefaultData originalData = new DefaultData(1, "foobar".getBytes());
        Packet originalPacket = new Packet(originalData, mock(PortableContext.class));

        ByteBuffer bb = ByteBuffer.allocate(100);
        boolean written = originalPacket.writeTo(bb);
        assertTrue(written);

        bb.flip();

        Packet clonedPacket = new Packet(mock(PortableContext.class));
        boolean read = clonedPacket.readFrom(bb);
        assertTrue(read);

        assertEquals(originalPacket.getHeader(), clonedPacket.getHeader());
        assertEquals(originalPacket.getData(), clonedPacket.getData());
    }
}

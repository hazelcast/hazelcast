package com.hazelcast.nio;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test that verifies that a packet can safely be stored in a byte-buffer and converted back
 * again into a packet.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PacketTransportTest extends HazelcastTestSupport {

    // check if the packet can deal with a buffer that is very small, but the data is very large.
    // This means that repeated calls to packet.write/packet.read are needed.
    @Test
    public void largeValue() {
        Packet originalPacket = new Packet(generateRandomString(100000).getBytes());

        Packet clonedPacket = new Packet();

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

        assertPacketEquals(originalPacket, clonedPacket);
    }

    @Test
    public void lotsOfPackets() {
        List<Packet> originalPackets = new LinkedList<Packet>();
        Random random = new Random();
        for (int k = 0; k < 1000; k++) {
            byte[] bytes = generateRandomString(random.nextInt(1000) + 8).getBytes();
            Packet originalPacket = new Packet(bytes);
            originalPackets.add(originalPacket);
        }

        ByteBuffer bb = ByteBuffer.allocate(20);

        for (Packet originalPacket : originalPackets) {
            Packet clonedPacket = new Packet();
            boolean writeCompleted;
            boolean readCompleted;
            do {
                writeCompleted = originalPacket.writeTo(bb);
                bb.flip();
                readCompleted = clonedPacket.readFrom(bb);
                bb.clear();
            } while (!writeCompleted);

            assertTrue(readCompleted);
            assertPacketEquals(originalPacket, clonedPacket);
        }
    }

    // This test verifies that writing a Packet to a ByteBuffer and then reading it from the ByteBuffer, gives the
    // same Packet (content).
    @Test
    public void cloningOfPacket() {
        Packet originalPacket = new Packet("foobarbaz".getBytes());

        ByteBuffer bb = ByteBuffer.allocate(100);
        boolean written = originalPacket.writeTo(bb);
        assertTrue(written);

        bb.flip();

        Packet clonedPacket = new Packet();
        boolean read = clonedPacket.readFrom(bb);
        assertTrue(read);

        assertPacketEquals(originalPacket, clonedPacket);
    }

    private void assertPacketEquals(Packet originalPacket, Packet clonedPacket) {
        assertEquals(originalPacket.getHeader(), clonedPacket.getHeader());
        assertArrayEquals(originalPacket.toByteArray(), clonedPacket.toByteArray());
    }
}

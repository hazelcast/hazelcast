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

    /**
     * Checks if the packet can deal with a buffer that is very small, but the data is very large, which
     * needs repeated calls to {@link Packet#writeTo(ByteBuffer)} and {@link Packet#readFrom(ByteBuffer)}.
     */
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

    /**
     * Verifies that writing a Packet to a ByteBuffer and then reading it from the ByteBuffer, gives the same Packet (content).
     */
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

    private static void assertPacketEquals(Packet originalPacket, Packet clonedPacket) {
        assertEquals(originalPacket.getFlags(), clonedPacket.getFlags());
        assertArrayEquals(originalPacket.toByteArray(), clonedPacket.toByteArray());
    }
}

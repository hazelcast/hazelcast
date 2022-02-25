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

package com.hazelcast.nio;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.internal.serialization.impl.SerializationConcurrencyTest;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static com.hazelcast.internal.serialization.impl.SerializationConcurrencyTest.FACTORY_ID;
import static com.hazelcast.internal.serialization.impl.SerializationConcurrencyTest.Person;
import static com.hazelcast.internal.serialization.impl.SerializationConcurrencyTest.PortableAddress;
import static com.hazelcast.internal.serialization.impl.SerializationConcurrencyTest.PortablePerson;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test that verifies that a packet can safely be stored in a byte-buffer and converted back
 * again into a packet.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PacketIOHelperTest extends HazelcastTestSupport {

    private PacketIOHelper packetWriter;
    private PacketIOHelper packetReader;

    private final Person person
            = new Person(111, 123L, 89.56d, "test-person", new SerializationConcurrencyTest.Address("street", 987));

    private final PortablePerson portablePerson = new PortablePerson(222, 456L, "portable-person",
            new PortableAddress("street", 567));

    @Before
    public void before() {
        packetWriter = new PacketIOHelper();
        packetReader = new PacketIOHelper();
    }

    private SerializationServiceBuilder createSerializationServiceBuilder() {
        PortableFactory portableFactory = new PortableFactory() {
            @Override
            public Portable create(int classId) {
                switch (classId) {
                    case 1:
                        return new PortablePerson();
                    case 2:
                        return new PortableAddress();
                    default:
                        throw new IllegalArgumentException();
                }
            }
        };
        return new DefaultSerializationServiceBuilder().addPortableFactory(FACTORY_ID, portableFactory);
    }

    @Test
    public void testPacketWriteRead() throws IOException {
        testPacketWriteRead(person);
    }

    @Test
    public void testPacketWriteRead_usingPortable() throws IOException {
        testPacketWriteRead(portablePerson);
    }

    private void testPacketWriteRead(Object originalObject) throws IOException {
        InternalSerializationService ss = createSerializationServiceBuilder().build();
        byte[] originalPayload = ss.toBytes(originalObject);

        ByteBuffer buffer = ByteBuffer.allocate(originalPayload.length * 2);
        Packet originalPacket = new Packet(originalPayload);
        assertTrue(packetWriter.writeTo(originalPacket, buffer));
        upcast(buffer).flip();

        SerializationService ss2 = createSerializationServiceBuilder().build();
        Packet clonedPacket = packetReader.readFrom(buffer);
        assertNotNull(clonedPacket);

        Object clonedObject = ss2.toObject(clonedPacket);

        assertEquals(originalPacket, clonedPacket);
        assertEquals(originalObject, clonedObject);
    }


    /**
     * Checks if the packet can deal with a buffer that is very small, but the data is very large, which
     * needs repeated calls to {@link PacketIOHelper#writeTo(Packet, ByteBuffer)} and
     * {@link PacketIOHelper#readFrom(ByteBuffer)}.
     */
    @Test
    public void largeValue() {
        Packet originalPacket = new Packet(generateRandomString(100000).getBytes());

        Packet clonedPacket;
        ByteBuffer bb = ByteBuffer.allocate(20);
        boolean writeCompleted;
        do {
            writeCompleted = packetWriter.writeTo(originalPacket, bb);
            upcast(bb).flip();
            clonedPacket = packetReader.readFrom(bb);
            upcast(bb).clear();
        } while (!writeCompleted);

        assertNotNull(clonedPacket);

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
            Packet clonedPacket;
            boolean writeCompleted;
            do {
                writeCompleted = packetWriter.writeTo(originalPacket, bb);
                upcast(bb).flip();
                clonedPacket = packetReader.readFrom(bb);
                upcast(bb).clear();
            } while (!writeCompleted);

            assertNotNull(clonedPacket);
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
        boolean written = packetWriter.writeTo(originalPacket, bb);
        assertTrue(written);

        upcast(bb).flip();

        Packet clonedPacket = packetReader.readFrom(bb);
        assertNotNull(clonedPacket);

        assertPacketEquals(originalPacket, clonedPacket);
    }

    private static void assertPacketEquals(Packet originalPacket, Packet clonedPacket) {
        assertEquals(originalPacket.getFlags(), clonedPacket.getFlags());
        assertArrayEquals(originalPacket.toByteArray(), clonedPacket.toByteArray());
    }
}

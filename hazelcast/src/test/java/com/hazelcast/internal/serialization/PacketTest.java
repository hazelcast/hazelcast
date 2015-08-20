/*
* Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.serialization.SerializationConcurrencyTest.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PacketTest {

    private final Person person = new Person(111, 123L, 89.56d, "test-person", new Address("street", 987));

    private final PortablePerson portablePerson = new PortablePerson(222, 456L, "portable-person", new PortableAddress("street", 567));

    private SerializationServiceBuilder createSerializationServiceBuilder() {
        final PortableFactory portableFactory = new PortableFactory() {
            public Portable create(int classId) {
                switch (classId) {
                    case 1:
                        return new PortablePerson();
                    case 2:
                        return new PortableAddress();
                }
                throw new IllegalArgumentException();
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
        SerializationService ss = createSerializationServiceBuilder().build();
        byte[] originalPayload = ss.toBytes(originalObject);

        ByteBuffer buffer = ByteBuffer.allocate(originalPayload.length * 2);
        Packet originalPacket = new Packet(originalPayload);
        assertTrue(originalPacket.writeTo(buffer));
        buffer.flip();

        SerializationService ss2 = createSerializationServiceBuilder().build();
        Packet clonedPacket = new Packet();
        assertTrue(clonedPacket.readFrom(buffer));

        Object clonedObject = ss2.toObject(clonedPacket);

        assertEquals(originalPacket, clonedPacket);
        assertEquals(originalObject, clonedObject);
    }
}

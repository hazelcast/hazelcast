package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SocketClientDataWriterTest {

    private SerializationService serializationService;
    private SocketClientDataWriter writer;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        writer = new SocketClientDataWriter();
    }

    @Test
    public void test() throws Exception {
        Packet packet = new Packet(serializationService.toBytes("foobar"));
        ByteBuffer bb = ByteBuffer.allocate(1000);
        boolean result = writer.write(packet, bb);

        assertTrue(result);

        // now we read out the bb and check if we can find the written packet.
        bb.flip();
        Packet resultPacket = new Packet();
        resultPacket.readFrom(bb);
        assertEquals(packet, resultPacket);
    }
}

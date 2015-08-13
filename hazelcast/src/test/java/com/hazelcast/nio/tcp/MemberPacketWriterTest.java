package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberPacketWriterTest extends HazelcastTestSupport {

    private SerializationService serializationService;
    private MemberPacketWriter writer;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        writer = new MemberPacketWriter();
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

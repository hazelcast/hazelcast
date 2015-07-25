package com.hazelcast.nio;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class OperationResponsePacketTest extends HazelcastTestSupport {

    @Test
    public void test() {
        Packet original = new Packet(generateRandomString(100).getBytes());
        original.setHeader(Packet.HEADER_OP);
        original.setHeader(Packet.HEADER_RESPONSE);
        original.setResponseType(Packet.RESPONSE_ERROR);
        original.setResponseCallId(200);
        original.setResponseSyncBackupCount(2);

        Packet clone = clone(original);

        assertEquals(original.getHeader(), clone.getHeader());
        assertEquals(original.getResponseType(), clone.getResponseType());
        assertEquals(original.getResponseCallId(), clone.getResponseCallId());
        assertEquals(original.getResponseSyncBackupCount(), clone.getResponseSyncBackupCount());
        assertArrayEquals(original.toByteArray(), clone.toByteArray());
    }

    private Packet clone(Packet originalPacket) {
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
        return clonedPacket;
    }
}

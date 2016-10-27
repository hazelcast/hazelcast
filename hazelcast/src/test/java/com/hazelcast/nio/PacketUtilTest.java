package com.hazelcast.nio;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import org.junit.Test;

import static com.hazelcast.nio.PacketUtil.isFlagSet;
import static com.hazelcast.nio.PacketUtil.toBytePacket;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PacketUtilTest {

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    public static final String PAYLOAD = "foobar";

    @Test
    public void testToPacket() {
        Packet packet = new Packet(serializationService.toBytes(PAYLOAD))
                .setFlag(Packet.FLAG_BIND);

        Packet found = PacketUtil.toPacket(PacketUtil.fromPacket(packet));
        assertEquals(packet, found);
    }

    @Test
    public void toPacket2() {
        Packet packet = new Packet(serializationService.toBytes(PAYLOAD))
                .setFlag(Packet.FLAG_BIND);


        byte[] bytes = toBytePacket(serializationService, PAYLOAD, Packet.FLAG_BIND, 0);
        assertEquals(packet.toByteArray().length, PacketUtil.getSize(bytes));
    }

    @Test
    public void testIsFlagSet() {

        byte[] bytes = toBytePacket(serializationService, PAYLOAD, Packet.FLAG_OP, 0);

        assertTrue(isFlagSet(bytes, Packet.FLAG_OP));
        assertFalse(isFlagSet(bytes, Packet.FLAG_BIND));
    }

    @Test
    public void testGetPartition_withPartitionId() {
        byte[] bytes = toBytePacket(serializationService, PAYLOAD, Packet.FLAG_OP, 10);
        assertEquals(10, PacketUtil.getPartition(bytes));
    }

    @Test
    public void testGetPartition_withoutPartitionId() {
        String payload = "foobar";

        byte[] bytes = toBytePacket(serializationService, payload, Packet.FLAG_OP, -1);
        assertEquals(-1, PacketUtil.getPartition(bytes));
    }
}

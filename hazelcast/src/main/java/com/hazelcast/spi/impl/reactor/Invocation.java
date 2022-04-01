package com.hazelcast.spi.impl.reactor;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutput;

import java.util.concurrent.CompletableFuture;

public class Invocation {

    public final static int OFFSET_OPCODE = Packet.DATA_OFFSET;
    public final static int OFFSET_CALL_ID = OFFSET_OPCODE+ Bits.BYTE_SIZE_IN_BYTES;

    public byte opcode;
    public int partitionId = -1;
    public Address target;
    public ByteArrayObjectDataOutput out;
    public long callId;
    public CompletableFuture future = new CompletableFuture();

    public Packet toPacket() {
        Packet packet = new Packet(out.toByteArray(), partitionId);
        packet.setPacketType(Packet.Type.OPERATION);
        return packet;
    }
}

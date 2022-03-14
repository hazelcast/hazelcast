package com.hazelcast.spi.impl.reactor;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutput;

public class Request {
    public byte opcode;
    public int partitionId = -1;
    public Address target;
    public Invocation invocation;
    public ByteArrayObjectDataOutput out;

    public Packet toPacket() {
        Packet packet = new Packet(out.toByteArray(), partitionId);
        packet.setPacketType(Packet.Type.OPERATION);
        return packet;
    }
}

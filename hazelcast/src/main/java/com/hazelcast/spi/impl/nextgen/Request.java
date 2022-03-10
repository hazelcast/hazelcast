package com.hazelcast.spi.impl.nextgen;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Packet;

public class Request {
    public byte opcode;
    public int partitionId=-1;
    public Address target;

    public Packet toPacket(){
        Packet packet = new Packet();
        return packet;
    }
}

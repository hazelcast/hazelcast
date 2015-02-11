package com.hazelcast.spi.impl;

import com.hazelcast.nio.Packet;

public interface ResponsePacketHandler {

    Response deserialize(Packet packet) throws Exception;

    void process(Response task) throws Exception;
}

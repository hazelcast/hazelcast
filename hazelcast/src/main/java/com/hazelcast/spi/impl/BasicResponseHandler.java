package com.hazelcast.spi.impl;

import com.hazelcast.nio.Packet;

public interface BasicResponseHandler {

    Response deserialize(Packet packet) throws Exception;

    void process(Response task) throws Exception;
}

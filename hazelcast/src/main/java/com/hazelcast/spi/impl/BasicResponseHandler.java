package com.hazelcast.spi.impl;

import com.hazelcast.nio.Packet;

public interface BasicResponseHandler {

    void process(Packet task) throws Exception;
}

package com.hazelcast.spi.impl.operationexecutor;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.Response;

/**
 * The {@link com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler} is responsible for handling
 * response packets.
 */
public interface ResponsePacketHandler {

    Response deserialize(Packet packet) throws Exception;

    void process(Response task) throws Exception;
}

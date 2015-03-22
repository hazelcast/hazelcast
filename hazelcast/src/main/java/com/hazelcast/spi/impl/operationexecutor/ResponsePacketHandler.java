package com.hazelcast.spi.impl.operationexecutor;

import com.hazelcast.nio.Packet;

/**
 * The {@link com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler} is responsible for handling
 * response packets.
 */
public interface ResponsePacketHandler {

    /**
     * Signals the ResponsePacketHandler that there is a response packet that should be handled.
     *
     * @param packet the response packet to handle
     * @throws Exception
     */
    void handle(Packet packet) throws Exception;
}

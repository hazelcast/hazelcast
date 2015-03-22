package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

/**
 * Responsible for handling responses.
 */
final class ResponsePacketHandlerImpl implements ResponsePacketHandler {

    private OperationServiceImpl operationService;

    public ResponsePacketHandlerImpl(OperationServiceImpl operationService) {
        this.operationService = operationService;
    }

    @Override
    public Response deserialize(Packet packet) throws Exception {
        Data data = packet.getData();
        return (Response) operationService.nodeEngine.toObject(data);
    }

    @Override
    public void process(Response response) throws Exception {
        try {
            operationService.invocationRegistry.notify(response);
        } catch (Throwable e) {
            operationService.logger.severe("While processing response...", e);
        }
    }
}

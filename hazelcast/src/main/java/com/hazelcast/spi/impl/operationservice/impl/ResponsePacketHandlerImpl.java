package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

/**
 * Responsible for handling responses.
 */
final class ResponsePacketHandlerImpl implements ResponsePacketHandler {

    private final ILogger logger;
    private final OperationServiceImpl operationService;
    private final NodeEngineImpl nodeEngine;

    public ResponsePacketHandlerImpl(OperationServiceImpl operationService) {
        this.operationService = operationService;
        this.logger = operationService.logger;
        this.nodeEngine = operationService.nodeEngine;
    }

    @Override
    public void handle(Packet packet) throws Exception {
        Data data = packet.getData();
        Response response = (Response) nodeEngine.toObject(data);
        try {
            operationService.invocationsRegistry.notify(response);
        } catch (Throwable e) {
            logger.severe("While processing response...", e);
        }
    }
}

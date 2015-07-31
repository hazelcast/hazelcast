/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

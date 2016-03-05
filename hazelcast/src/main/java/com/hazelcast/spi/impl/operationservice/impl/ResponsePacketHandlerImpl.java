/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;

import static com.hazelcast.spi.impl.SpiDataSerializerHook.BACKUP_RESPONSE;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.CALL_TIMEOUT_RESPONSE;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.ERROR_RESPONSE;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.NORMAL_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.backupCount;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.callId;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.typeId;

/**
 * Responsible for handling responses.
 */
final class ResponsePacketHandlerImpl implements PacketHandler {

    private final ILogger logger;
    private final InternalSerializationService serializationService;
    private final InvocationRegistry invocationRegistry;

    public ResponsePacketHandlerImpl(ILogger logger,
                                     InternalSerializationService serializationService,
                                     InvocationRegistry invocationRegistry) {
        this.logger = logger;
        this.serializationService = serializationService;
        this.invocationRegistry = invocationRegistry;
    }

    @Override
    public void handle(Packet packet) throws Exception {
        Address sender = packet.getConn().getEndPoint();

        byte[] bytes = packet.toByteArray();
        int typeId = typeId(bytes);
        try {
            switch (typeId) {
                case NORMAL_RESPONSE:
                    invocationRegistry.notifyNormalResponse(
                            callId(bytes),
                            packet,
                            backupCount(bytes),
                            sender);
                    break;
                case BACKUP_RESPONSE:
                    invocationRegistry.notifyBackupComplete(callId(bytes));
                    break;
                case ERROR_RESPONSE:
                    invocationRegistry.notifyErrorResponse(
                            callId(bytes),
                            packet,
                            sender);
                    break;
                case CALL_TIMEOUT_RESPONSE:
                    invocationRegistry.notifyCallTimeout(callId(bytes), sender);
                    break;
                default:
                    throw new IllegalStateException("Unrecognized response typeId:" + typeId);
            }
        } catch (Throwable e) {
            logger.severe("While processing response...", e);
        }
    }

}

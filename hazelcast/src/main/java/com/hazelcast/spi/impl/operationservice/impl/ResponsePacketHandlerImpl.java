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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PacketHandler;

/**
 * Responsible for handling responses.
 */
public class ResponsePacketHandlerImpl implements PacketHandler {

    private final ILogger logger;
    private final InvocationRegistry invocationRegistry;

    public ResponsePacketHandlerImpl(ILogger logger, InvocationRegistry invocationRegistry) {
        this.logger = logger;
        this.invocationRegistry = invocationRegistry;
    }

    @Override
    public void handle(Packet packet) throws Exception {
        try {
            long callId = packet.getResponseCallId();
            Address sender = packet.getConn().getEndPoint();
            switch (packet.getResponseType()) {
                case Packet.RESPONSE_NORMAL:
                    Data response = packet.totalSize() == 0 ? null : packet;
                    invocationRegistry.notifyNormalResponse(callId, packet.getResponseSyncBackupCount(), response, sender);
                    break;
                case Packet.RESPONSE_ERROR:
                    invocationRegistry.notifyErrorResponse(callId, packet, sender);
                    break;
                case Packet.RESPONSE_BACKUP:
                    invocationRegistry.notifyBackupComplete(callId, sender);
                    break;
                case Packet.RESPONSE_TIMEOUT:
                    invocationRegistry.notifyCallTimeout(callId, sender);
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized Response Packet: " + packet);
            }
        } catch (Throwable e) {
            logger.severe("Error Processing Response Packet:" + packet, e);
        }
    }
}

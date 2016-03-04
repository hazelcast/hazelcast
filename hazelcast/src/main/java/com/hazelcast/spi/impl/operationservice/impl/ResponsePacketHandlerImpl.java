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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;

import static com.hazelcast.nio.Bits.readLong;
import static com.hazelcast.spi.impl.operationservice.impl.RemoteInvocationResponseHandler.TYPE_BACKUP_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.RemoteInvocationResponseHandler.TYPE_ERROR_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.RemoteInvocationResponseHandler.TYPE_NORMAL_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.RemoteInvocationResponseHandler.TYPE_TIMEOUT_RESPONSE;

/**
 * Responsible for handling responses.
 */
final class ResponsePacketHandlerImpl implements PacketHandler {

    private final ILogger logger;
    private final InvocationRegistry invocationRegistry;

    public ResponsePacketHandlerImpl(ILogger logger, InvocationRegistry invocationRegistry) {
        this.logger = logger;
        this.invocationRegistry = invocationRegistry;
    }

    @Override
    public void handle(Packet packet) throws Exception {
        Address sender = packet.getConn().getEndPoint();
        byte[] bytes = packet.toByteArray();

        byte type = bytes[bytes.length - 1];
        long callId = readLong(bytes, bytes.length - 9, true);
        try {
            switch (type) {
                case TYPE_NORMAL_RESPONSE:
                    int backupCount = bytes[bytes.length - 10];
                    invocationRegistry.notifyNormalResponse(callId, packet, backupCount, sender);
                    break;
                case TYPE_BACKUP_RESPONSE:
                    invocationRegistry.notifyBackupComplete(callId);
                    break;
                case TYPE_TIMEOUT_RESPONSE:
                    invocationRegistry.notifyCallTimeout(callId, sender);
                    break;
                case TYPE_ERROR_RESPONSE:
                    invocationRegistry.notifyErrorResponse(callId, packet, sender);
                    break;
                default:
                    throw new IllegalStateException("Unrecognized response type:" + type);
            }
        } catch (Throwable e) {
            logger.severe("While processing response...", e);
        }
    }
}

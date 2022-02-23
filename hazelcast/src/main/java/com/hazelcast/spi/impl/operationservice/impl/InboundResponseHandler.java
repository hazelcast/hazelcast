/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;

import java.nio.ByteOrder;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.internal.nio.Packet.FLAG_OP_RESPONSE;
import static com.hazelcast.internal.nio.Packet.Type.OPERATION;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.BACKUP_ACK_RESPONSE;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.CALL_TIMEOUT_RESPONSE;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.ERROR_RESPONSE;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.NORMAL_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse.OFFSET_BACKUP_ACKS;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_CALL_ID;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_TYPE_ID;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Responsible for handling responses for invocations. Based on the content of the
 * response packet, it will lookup the Invocation from the InvocationRegistry and
 * notify the Invocation.
 *
 * InboundResponseHandlers are not thread-safe. So if there are multiple threads
 * processing responses, each thread needs to get its own instance. Only the backup
 * handling is thread-safe since backups can be completed locally by any thread.
 */
public final class InboundResponseHandler implements Consumer<Packet> {

    final SwCounter responsesNormal = newSwCounter();
    final SwCounter responsesTimeout = newSwCounter();
    final MwCounter responsesBackup = newMwCounter();
    final SwCounter responsesError = newSwCounter();
    final MwCounter responsesMissing = newMwCounter();
    private final ILogger logger;
    private final InternalSerializationService serializationService;
    private final InvocationRegistry invocationRegistry;
    private final NodeEngine nodeEngine;
    private final boolean useBigEndian;

    InboundResponseHandler(InvocationRegistry invocationRegistry, NodeEngine nodeEngine) {
        this.logger = nodeEngine.getLogger(InboundResponseHandler.class);
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.useBigEndian = serializationService.getByteOrder() == ByteOrder.BIG_ENDIAN;
        this.invocationRegistry = invocationRegistry;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void accept(Packet packet) {
        checkNotNull(packet, "packet can't be null");
        checkTrue(packet.getPacketType() == OPERATION, "Packet type is not OPERATION");
        checkTrue(packet.isFlagRaised(FLAG_OP_RESPONSE), "FLAG_OP_RESPONSE is not set");

        byte[] bytes = packet.toByteArray();
        int typeId = Bits.readInt(bytes, OFFSET_TYPE_ID, useBigEndian);
        long callId = Bits.readLong(bytes, OFFSET_CALL_ID, useBigEndian);
        Address sender = packet.getConn().getRemoteAddress();
        try {
            switch (typeId) {
                case NORMAL_RESPONSE:
                    byte backupAcks = bytes[OFFSET_BACKUP_ACKS];
                    notifyNormalResponse(callId, packet, backupAcks, sender);
                    break;
                case BACKUP_ACK_RESPONSE:
                    notifyBackupComplete(callId);
                    break;
                case CALL_TIMEOUT_RESPONSE:
                    notifyCallTimeout(callId, sender);
                    break;
                case ERROR_RESPONSE:
                    ErrorResponse errorResponse = serializationService.toObject(packet);
                    notifyErrorResponse(callId, errorResponse.getCause(), sender);
                    break;
                default:
                    logger.severe("Unrecognized type: " + typeId + " packet:" + packet);
            }
        } catch (Throwable e) {
            logger.severe("While processing response...", e);
        }
    }

    public void notifyBackupComplete(long callId) {
        responsesBackup.inc();

        try {
            Invocation invocation = invocationRegistry.get(callId);

            // It can happen that a backup response is send without the Invocation being available anymore.
            // This is because the InvocationRegistry will automatically release invocations where the backup is
            // taking too much time.
            if (invocation == null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("No Invocation found for backup response with callId=" + callId);
                }
                return;
            }

            invocation.notifyBackupComplete();
        } catch (Exception e) {
            ReplicaErrorLogger.log(e, logger);
        }
    }

    void notifyErrorResponse(long callId, Object cause, Address sender) {
        responsesError.inc();
        Invocation invocation = invocationRegistry.get(callId);

        if (invocation == null) {
            responsesMissing.inc();
            if (nodeEngine.isRunning() && callId != 0) {
                logger.warning("No Invocation found for error response with callId=" + callId + " sent from " + sender);
            }
            return;
        }

        invocation.notifyError(cause);
    }

    void notifyNormalResponse(long callId, Object value, int backupCount, Address sender) {
        responsesNormal.inc();
        Invocation invocation = invocationRegistry.get(callId);

        if (invocation == null) {
            responsesMissing.inc();
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for normal response with callId=" + callId + " sent from " + sender);
            }
            return;
        }
        invocation.notifyNormalResponse(value, backupCount);
    }

    void notifyCallTimeout(long callId, Address sender) {
        responsesTimeout.inc();
        Invocation invocation = invocationRegistry.get(callId);

        if (invocation == null) {
            responsesMissing.inc();
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for call timeout response with callId=" + callId + " sent from " + sender);
            }
            return;
        }
        invocation.notifyCallTimeout();
    }
}

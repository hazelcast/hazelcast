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

import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPool;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.OperationResponseHandler;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.handleException;
import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.nio.Packet.FLAG_URGENT;

/**
 * An {@link OperationResponseHandler} that is used for a remotely executed Operation. So when a calling member
 * sends an Operation to the receiving member, the receiving member attaches this RemoteOperationResponseHandler
 * to that operation.
 */
public final class RemoteOperationResponseHandler implements OperationResponseHandler {
    public static final byte TYPE_NORMAL_RESPONSE = 0;
    public static final byte TYPE_BACKUP_RESPONSE = 1;
    public static final byte TYPE_ERROR_RESPONSE = 2;
    public static final byte TYPE_TIMEOUT_RESPONSE = 3;

    private final OperationServiceImpl operationService;
    private final SerializationService serializationService;
    private final Node node;

    public RemoteOperationResponseHandler(OperationServiceImpl operationService, SerializationService serializationService) {
        this.operationService = operationService;
        this.serializationService = serializationService;
        this.node = operationService.nodeEngine.getNode();
    }

    @Override
    public void sendResponse(Connection receiver, boolean urgent, long callId, int backupCount, Object value) {
        Packet packet = buildResponsePacket(urgent, callId, backupCount, value);
        receiver.write(packet);
    }

    Packet buildResponsePacket(boolean urgent, long callId, int backupCount, Object value) {
        BufferPool pool = serializationService.pool();
        BufferObjectDataOutput out = pool.takeOutputBuffer();
        try {
            serializationService.writeAsData(value, out);

            // we can safely write a byte since backup count is smaller than Byte.MAX_VALUE.
            out.writeByte(backupCount);
            out.writeLong(callId);
            out.writeByte(TYPE_NORMAL_RESPONSE);

            byte[] bytes = out.toByteArray();
            Packet packet = new Packet(bytes, -1);
            packet.setFlag(FLAG_OP | FLAG_RESPONSE);
            if (urgent) {
                packet.setFlag(FLAG_URGENT);
            }

            //todo: originally the exception was logged.

            return packet;
        } catch (Throwable t) {
            throw handleException(t);
        } finally {
            pool.returnOutputBuffer(out);
        }
    }

    @Override
    public void sendErrorResponse(Connection receiver, boolean urgent, long callId, Throwable error) {
        Packet packet = buildErrorResponsePacket(urgent, callId, error);

        //todo: exception
        receiver.write(packet);
    }

    Packet buildErrorResponsePacket(boolean urgent, long callId, Throwable error) {
        BufferPool pool = serializationService.pool();
        BufferObjectDataOutput out = pool.takeOutputBuffer();
        try {
            serializationService.writeAsData(error, out);
            out.writeLong(callId);
            out.writeByte(TYPE_ERROR_RESPONSE);

            byte[] bytes = out.toByteArray();

            Packet packet = new Packet(bytes, -1);
            packet.setFlag(FLAG_OP | FLAG_RESPONSE);
            if (urgent) {
                packet.setFlag(FLAG_URGENT);
            }

            return packet;
        } catch (Throwable t) {
            throw handleException(t);
        } finally {
            pool.returnOutputBuffer(out);
        }
    }

    @Override
    public void sendTimeoutResponse(Connection receiver, boolean urgent, long callId) {
        Packet packet = buildTimeoutResponsePacket(urgent, callId);

        //todo: exception
        receiver.write(packet);
    }

    Packet buildTimeoutResponsePacket(boolean urgent, long callId) {
        BufferPool pool = serializationService.pool();
        BufferObjectDataOutput out = pool.takeOutputBuffer();
        try {
            out.writeLong(callId);
            out.writeByte(TYPE_TIMEOUT_RESPONSE);
            byte[] bytes = out.toByteArray();

            Packet packet = new Packet(bytes, -1);
            packet.setFlag(FLAG_OP | FLAG_RESPONSE);
            if (urgent) {
                packet.setFlag(FLAG_URGENT);
            }
            return packet;
        } catch (Throwable t) {
            throw handleException(t);
        } finally {
            pool.returnOutputBuffer(out);
        }
    }

    @Override
    public void sendBackupResponse(Address receiver, boolean urgent, long callId) {
        Packet packet = buildBackupResponsePacket(urgent, callId);

        ConnectionManager connectionManager = operationService.node.getConnectionManager();
        Connection connection = connectionManager.getOrConnect(receiver);
        //todo: exception
        connectionManager.transmit(packet, connection);
    }

    Packet buildBackupResponsePacket(boolean urgent, long callId) {
        BufferPool pool = serializationService.pool();
        BufferObjectDataOutput out = pool.takeOutputBuffer();
        try {
            out.writeLong(callId);
            out.writeByte(TYPE_BACKUP_RESPONSE);
            byte[] bytes = out.toByteArray();

            Packet packet = new Packet(bytes, -1);
            packet.setFlag(FLAG_OP | FLAG_RESPONSE);
            if (urgent) {
                packet.setFlag(FLAG_URGENT);
            }

            return packet;
        } catch (Throwable t) {
            throw handleException(t);
        } finally {
            pool.returnOutputBuffer(out);
        }
    }

    @Override
    public boolean isLocal() {
        return false;
    }
}

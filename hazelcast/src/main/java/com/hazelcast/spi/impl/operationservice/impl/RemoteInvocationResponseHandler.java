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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * An {@link OperationResponseHandler} that is used for a remotely executed Operation. So when a calling member
 * sends an Operation to the receiving member, the receiving member attaches this RemoteInvocationResponseHandler
 * to that operation.
 */
public final class RemoteInvocationResponseHandler implements OperationResponseHandler {

    private final SerializationService serializationService;
    private final NodeEngineImpl nodeEngine;

    public RemoteInvocationResponseHandler(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = nodeEngine.getSerializationService();
    }

    @Override
    public void sendNormalResponse(Operation op, Object response, int syncBackupCount) {
        byte[] payload = serializationService.toBytes(response);
        Packet packet = new Packet(payload);
        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);
        if (op.isUrgent()) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        packet.setResponseType(Packet.RESPONSE_NORMAL);
        packet.setResponseCallId(op.getCallId());
        packet.setResponseSyncBackupCount(syncBackupCount);

        Connection connection = op.getConnection();
        if (!transmit(connection, packet)) {
            throw new HazelcastException("Cannot send BackupResponse: " + op.getCallId() + " to " + connection.getEndPoint());
        }
    }

    @Override
    public void sendBackupComplete(Address address, long callId, boolean urgent) {
        Packet packet = new Packet(null);
        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);
        if (urgent) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        packet.setResponseType(Packet.RESPONSE_BACKUP);
        packet.setResponseCallId(callId);

        Connection connection = getConnection(address);
        if (!transmit(connection, packet)) {
            throw new HazelcastException("Cannot send BackupResponse: " + callId + " to " + connection.getEndPoint());
        }
    }

    @Override
    public void sendErrorResponse(Address address, long callId, boolean urgent, Operation op, Throwable cause) {
        byte[] payload = serializationService.toBytes(cause);
        Packet packet = new Packet(payload);
        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);
        if (urgent) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        packet.setResponseType(Packet.RESPONSE_ERROR);
        packet.setResponseCallId(callId);

        Connection connection = getConnection(address);
        if (!transmit(connection, packet)) {
            throw new HazelcastException("Cannot send BackupResponse: " + callId + " to " + connection.getEndPoint());
        }
    }

    @Override
    public void sendTimeoutResponse(Operation op) {
        Packet packet = new Packet(null);

        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);
        if (op.isUrgent()) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        packet.setResponseType(Packet.RESPONSE_TIMEOUT);
        packet.setResponseCallId(op.getCallId());

        Connection connection = op.getConnection();
        if (!transmit(connection, packet)) {
            throw new HazelcastException("Cannot send BackupResponse: " + op.getCallId() + " to " + connection.getEndPoint());
        }
    }

    private boolean transmit(Connection connection, Packet packet) {
        return nodeEngine.getNode().getConnectionManager().transmit(packet, connection);
    }

    private Connection getConnection(Address address) {
        return nodeEngine.getNode().connectionManager.getConnection(address);
    }

    @Override
    public boolean isLocal() {
        return false;
    }
}

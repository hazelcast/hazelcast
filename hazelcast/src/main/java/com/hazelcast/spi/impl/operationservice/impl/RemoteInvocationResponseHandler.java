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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

import static com.hazelcast.util.Preconditions.checkNotNull;

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
    public void sendResponse(Operation operation, Object obj) {
        Connection conn = operation.getConnection();

        Response response;
        if (obj instanceof Throwable) {
            response = new ErrorResponse((Throwable) obj, operation.getCallId(), operation.isUrgent());
        } else if (!(obj instanceof Response)) {
            response = new NormalResponse(obj, operation.getCallId(), 0, operation.isUrgent());
        } else {
            response = (Response) obj;
        }

        if (!send(operation, response)) {
            throw new HazelcastException("Cannot send response: " + obj + " to " + conn.getEndPoint());
        }
    }

    public boolean send(Operation op, Response response) {
        checkNotNull(op, "op can't be null");
        checkNotNull(response, "response can't be null");

        Connection connection = op.getConnection();
        return send(connection, response);
    }

    public boolean send(Connection connection, Response response) {
        checkNotNull(connection, "op can't be null");
        checkNotNull(response, "response can't be null");

        Data data = serializationService.toData(response);
        Packet packet = new Packet(data);
        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);

        if (response.isUrgent()) {
            packet.setHeader(Packet.HEADER_URGENT);
        }

        return nodeEngine.getPacketTransceiver().transmit(packet, connection);
    }

    @Override
    public boolean isLocal() {
        return false;
    }
}

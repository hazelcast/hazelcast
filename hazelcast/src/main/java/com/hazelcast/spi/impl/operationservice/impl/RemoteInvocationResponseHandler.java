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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * An {@link OperationResponseHandler} that is used for a remotely executed Operation. So when a calling member
 * sends an Operation to the receiving member, the receiving member attaches this RemoteInvocationResponseHandler
 * to that operation.
 */
public final class RemoteInvocationResponseHandler implements OperationResponseHandler {
    private final Address thisAddress;
    private final InternalSerializationService serializationService;
    private final ILogger logger;
    private final Node node;

    RemoteInvocationResponseHandler(
            Address thisAddress,
            InternalSerializationService serializationService,
            ILogger logger,
            Node node) {
        this.thisAddress = thisAddress;
        this.serializationService = serializationService;
        this.logger = logger;
        this.node = node;
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

        if (!send(response, operation.getCallerAddress())) {
            logger.warning("Cannot send response: " + obj + " to " + conn.getEndPoint()
                    + ". " + operation);
        }
    }

    public boolean send(Response response, Address target) {
        checkNotNull(target, "Target is required!");

        if (thisAddress.equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", response: " + response);
        }

        byte[] bytes = serializationService.toBytes(response);
        Packet packet = new Packet(bytes, -1)
                .setAllFlags(FLAG_OP | FLAG_RESPONSE);

        if (response.isUrgent()) {
            packet.setFlag(FLAG_URGENT);
        }

        ConnectionManager connectionManager = node.getConnectionManager();
        Connection connection = connectionManager.getOrConnect(target);
        return connectionManager.transmit(packet, connection);
    }
}

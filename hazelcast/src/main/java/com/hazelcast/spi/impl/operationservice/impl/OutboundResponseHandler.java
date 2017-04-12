/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

import java.nio.ByteOrder;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;
import static com.hazelcast.nio.Bits.writeInt;
import static com.hazelcast.nio.Bits.writeIntB;
import static com.hazelcast.nio.Bits.writeLong;
import static com.hazelcast.nio.Packet.FLAG_OP_RESPONSE;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static com.hazelcast.nio.Packet.Type.OPERATION;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.BACKUP_ACK_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.responses.BackupAckResponse.BACKUP_RESPONSE_SIZE_IN_BYTES;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_CALL_ID;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_IDENTIFIED;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_SERIALIZER_TYPE_ID;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_TYPE_FACTORY_ID;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_TYPE_ID;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_URGENT;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * An {@link OperationResponseHandler} that is used for a remotely executed Operation. So when a calling member
 * sends an Operation to the receiving member, the receiving member attaches this OutboundResponseHandler
 * to that operation so that the response is returned to the right machine.
 */
public final class OutboundResponseHandler implements OperationResponseHandler {

    private final Address thisAddress;
    private final InternalSerializationService serializationService;
    private final boolean useBigEndian;
    private final ILogger logger;
    // it sucks we need to pass in Node as argument; but this is due to the ConnectionManager which is created after
    // the OperationService is created.
    private final Node node;

    OutboundResponseHandler(Address thisAddress,
                            InternalSerializationService serializationService,
                            Node node,
                            ILogger logger) {
        this.thisAddress = thisAddress;
        this.serializationService = serializationService;
        this.useBigEndian = serializationService.getByteOrder() == ByteOrder.BIG_ENDIAN;
        this.node = node;
        this.logger = logger;
    }

    @Override
    public void sendResponse(Operation operation, Object obj) {
        Response response = toResponse(operation, obj);

        if (!send(response, operation.getCallerAddress())) {
            Connection conn = operation.getConnection();
            logger.warning("Cannot send response: " + obj + " to " + conn.getEndPoint()
                    + ". " + operation);
        }
    }

    private static Response toResponse(Operation operation, Object obj) {
        if (obj instanceof Throwable) {
            return new ErrorResponse((Throwable) obj, operation.getCallId(), operation.isUrgent());
        } else if (!(obj instanceof Response)) {
            return new NormalResponse(obj, operation.getCallId(), 0, operation.isUrgent());
        } else {
            return (Response) obj;
        }
    }

    public boolean send(Response response, Address target) {
        checkNotNull(target, "Target is required!");

        if (thisAddress.equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", response: " + response);
        }

        byte[] bytes = serializationService.toBytes(response);

        Packet packet = newResponsePacket(bytes, response.isUrgent());

        ConnectionManager connectionManager = node.getConnectionManager();
        Connection connection = connectionManager.getOrConnect(target);
        return connectionManager.transmit(packet, connection);
    }

    public void sendBackupAck(Address target, long callId, boolean urgent) {
        checkNotNull(target, "Target is required!");

        if (thisAddress.equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target);
        }

        byte[] bytes = new byte[BACKUP_RESPONSE_SIZE_IN_BYTES];

        writeResponseEpilogueBytes(bytes, BACKUP_ACK_RESPONSE, callId, urgent);

        Packet packet = newResponsePacket(bytes, urgent);

        ConnectionManager connectionManager = node.getConnectionManager();
        Connection connection = connectionManager.getOrConnect(target);
        connectionManager.transmit(packet, connection);
    }

    private void writeResponseEpilogueBytes(byte[] bytes, int typeId, long callId, boolean urgent) {
        // data-serializable type (this is always written with big endian)
        writeIntB(bytes, OFFSET_SERIALIZER_TYPE_ID, CONSTANT_TYPE_DATA_SERIALIZABLE);
        // identified or not
        bytes[OFFSET_IDENTIFIED] = 1;
        // factory id
        writeInt(bytes, OFFSET_TYPE_FACTORY_ID, SpiDataSerializerHook.F_ID, useBigEndian);
        // type id
        writeInt(bytes, OFFSET_TYPE_ID, typeId, useBigEndian);
        // call id
        writeLong(bytes, OFFSET_CALL_ID, callId, useBigEndian);
        // urgent
        bytes[OFFSET_URGENT] = (byte) (urgent ? 1 : 0);
    }

    private Packet newResponsePacket(byte[] bytes, boolean urgent) {
        Packet packet = new Packet(bytes, -1)
                .setPacketType(OPERATION)
                .raiseFlags(FLAG_OP_RESPONSE);

        if (urgent) {
            packet.raiseFlags(FLAG_URGENT);
        }
        return packet;
    }
}

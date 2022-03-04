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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_NULL;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.writeInt;
import static com.hazelcast.internal.nio.Bits.writeIntB;
import static com.hazelcast.internal.nio.Bits.writeLong;
import static com.hazelcast.internal.nio.Packet.FLAG_OP_RESPONSE;
import static com.hazelcast.internal.nio.Packet.FLAG_URGENT;
import static com.hazelcast.internal.nio.Packet.Type.OPERATION;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.BACKUP_ACK_RESPONSE;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.NORMAL_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.responses.BackupAckResponse.BACKUP_RESPONSE_SIZE_IN_BYTES;
import static com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse.OFFSET_BACKUP_ACKS;
import static com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse.OFFSET_DATA_LENGTH;
import static com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse.OFFSET_DATA_PAYLOAD;
import static com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse.OFFSET_IS_DATA;
import static com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse.OFFSET_NOT_DATA;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_CALL_ID;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_IDENTIFIED;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_SERIALIZER_TYPE_ID;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_TYPE_FACTORY_ID;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_TYPE_ID;
import static com.hazelcast.spi.impl.operationservice.impl.responses.Response.OFFSET_URGENT;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.nio.ByteOrder.BIG_ENDIAN;

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

    OutboundResponseHandler(Address thisAddress,
                            InternalSerializationService serializationService,
                            ILogger logger) {
        this.thisAddress = thisAddress;
        this.serializationService = serializationService;
        this.useBigEndian = serializationService.getByteOrder() == BIG_ENDIAN;
        this.logger = logger;
    }

    @Override
    public void sendResponse(Operation operation, Object obj) {
        Address target = operation.getCallerAddress();
        ServerConnectionManager connectionManager = operation.getConnection().getConnectionManager();
        boolean send;
        if (obj == null) {
            send = sendNormalResponse(connectionManager, target, operation.getCallId(), 0, operation.isUrgent(), null);
        } else if (obj.getClass() == NormalResponse.class) {
            NormalResponse response = (NormalResponse) obj;
            send = sendNormalResponse(connectionManager, target, response.getCallId(),
                    response.getBackupAcks(), response.isUrgent(), response.getValue());
        } else if (obj.getClass() == ErrorResponse.class || obj.getClass() == CallTimeoutResponse.class) {
            send = send(connectionManager, target, (Response) obj);
        } else if (obj instanceof Throwable) {
            send = send(connectionManager, target, new ErrorResponse((Throwable) obj,
                    operation.getCallId(), operation.isUrgent()));
        } else {
            // most regular responses not wrapped in a NormalResponse. So we are now completely skipping the
            // NormalResponse instance
            send = sendNormalResponse(connectionManager, target, operation.getCallId(), 0, operation.isUrgent(), obj);
        }

        if (!send) {
            logger.warning("Cannot send response: " + obj + " to " + target + ". " + operation);
        }
    }

    public boolean send(ServerConnectionManager connectionManager, Address target, Response response) {
        checkNotNull(target, "Target is required!");

        if (thisAddress.equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", response: " + response);
        }

        byte[] bytes = serializationService.toBytes(response);

        Packet packet = newResponsePacket(bytes, response.isUrgent());

        return transmit(target, packet, connectionManager);
    }

    private boolean sendNormalResponse(ServerConnectionManager connectionManager, Address target, long callId,
                                       int backupAcks, boolean urgent, Object value) {
        checkTarget(target);

        Packet packet = toNormalResponsePacket(callId, (byte) backupAcks, urgent, value);

        return transmit(target, packet, connectionManager);
    }

    Packet toNormalResponsePacket(long callId, int backupAcks, boolean urgent, Object value) {
        byte[] bytes;
        boolean isData = value instanceof Data;
        if (isData) {
            Data data = (Data) value;

            int dataLengthInBytes = data.totalSize();
            bytes = new byte[OFFSET_DATA_PAYLOAD + dataLengthInBytes];
            writeInt(bytes, OFFSET_DATA_LENGTH, dataLengthInBytes, useBigEndian);

            // this is a crucial part. If data is NativeMemoryData, instead of calling Data.toByteArray which causes a
            // byte-array to be created and a intermediate copy of the data, we immediately copy the NativeMemoryData
            // into the bytes for the packet.
            data.copyTo(bytes, OFFSET_DATA_PAYLOAD);
        } else if (value == null) {
            // since there are many 'null' responses we optimize this case as well.
            bytes = new byte[OFFSET_NOT_DATA + INT_SIZE_IN_BYTES];
            writeInt(bytes, OFFSET_NOT_DATA, CONSTANT_TYPE_NULL, useBigEndian);
        } else {
            // for regular object we currently can't guess how big the bytes will be; so we just hand it
            // over to the serializationService to deal with it. The negative part is that this does lead to
            // an intermediate copy of the data.

            bytes = serializationService.toBytes(value, OFFSET_NOT_DATA, false);
        }

        writeResponsePrologueBytes(bytes, NORMAL_RESPONSE, callId, urgent);

        // backup-acks (will fit in a byte)
        bytes[OFFSET_BACKUP_ACKS] = (byte) backupAcks;
        // isData
        bytes[OFFSET_IS_DATA] = (byte) (isData ? 1 : 0);
        //the remaining part of the byte array is already filled, so we are done.

        return newResponsePacket(bytes, urgent);
    }

    public void sendBackupAck(ServerConnectionManager connectionManager, Address target, long callId, boolean urgent) {
        checkTarget(target);

        Packet packet = toBackupAckPacket(callId, urgent);

        transmit(target, packet, connectionManager);
    }

    Packet toBackupAckPacket(long callId, boolean urgent) {
        byte[] bytes = new byte[BACKUP_RESPONSE_SIZE_IN_BYTES];

        writeResponsePrologueBytes(bytes, BACKUP_ACK_RESPONSE, callId, urgent);

        return newResponsePacket(bytes, urgent);
    }

    private void writeResponsePrologueBytes(byte[] bytes, int typeId, long callId, boolean urgent) {
        // partition hash (which is always 0 in case of response)
        writeIntB(bytes, 0, 0);
        // data-serializable type (this is always written with big endian)
        writeIntB(bytes, OFFSET_SERIALIZER_TYPE_ID, CONSTANT_TYPE_DATA_SERIALIZABLE);
        // identified or not
        bytes[OFFSET_IDENTIFIED] = 1;
        // factory ID
        writeInt(bytes, OFFSET_TYPE_FACTORY_ID, SpiDataSerializerHook.F_ID, useBigEndian);
        // type ID
        writeInt(bytes, OFFSET_TYPE_ID, typeId, useBigEndian);
        // call ID
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

    private boolean transmit(Address target, Packet packet, ServerConnectionManager connectionManager) {
        // The response is send over an arbitrary stream id. It needs to be arbitrary so that
        // responses don't end up at stream 0 and the connection this stream belongs to, becomes
        // a bottleneck.
        // The order of operations is respected, but the order of responses is not respected, e.g.
        // for inbound responses we toss responses in an arbitrary response thread.
        return connectionManager.transmit(packet, target,  ThreadLocalRandom.current().nextInt());
    }

    private void checkTarget(Address target) {
        checkNotNull(target, "Target is required!");

        if (thisAddress.equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target);
        }
    }
}

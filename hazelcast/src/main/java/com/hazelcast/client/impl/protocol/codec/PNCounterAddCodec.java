/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/**
 * Adds a delta to the PNCounter value. The delta may be negative for a
 * subtraction.
 * <p>
 * The invocation will return the replica timestamps (vector clock) which
 * can then be sent with the next invocation to keep session consistency
 * guarantees.
 * The target replica is determined by the {@code targetReplica} parameter.
 * If smart routing is disabled, the actual member processing the client
 * message may act as a proxy.
 */
public final class PNCounterAddCodec {
    //hex: 0x200200
    public static final int REQUEST_MESSAGE_TYPE = 2097664;
    //hex: 0x200201
    public static final int RESPONSE_MESSAGE_TYPE = 2097665;
    private static final int REQUEST_DELTA_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_GET_BEFORE_UPDATE_FIELD_OFFSET = REQUEST_DELTA_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_GET_BEFORE_UPDATE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_VALUE_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_REPLICA_COUNT_FIELD_OFFSET = RESPONSE_VALUE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_REPLICA_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private PNCounterAddCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * the name of the PNCounter
         */
        public java.lang.String name;

        /**
         * the delta to add to the counter value, can be negative
         */
        public long delta;

        /**
         * {@code true} if the operation should return the
         * counter value before the addition, {@code false}
         * if it should return the value after the addition
         */
        public boolean getBeforeUpdate;

        /**
         * last observed replica timestamps (vector clock)
         */
        public java.util.List<java.util.Map.Entry<java.lang.String, java.lang.Long>> replicaTimestamps;

        /**
         * the target replica
         */
        public com.hazelcast.nio.Address targetReplica;
    }

    public static ClientMessage encodeRequest(java.lang.String name, long delta, boolean getBeforeUpdate, java.util.Collection<java.util.Map.Entry<java.lang.String, java.lang.Long>> replicaTimestamps, com.hazelcast.nio.Address targetReplica) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("PNCounter.Add");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeLong(initialFrame.content, REQUEST_DELTA_FIELD_OFFSET, delta);
        encodeBoolean(initialFrame.content, REQUEST_GET_BEFORE_UPDATE_FIELD_OFFSET, getBeforeUpdate);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        MapStringLongCodec.encode(clientMessage, replicaTimestamps);
        AddressCodec.encode(clientMessage, targetReplica);
        return clientMessage;
    }

    public static PNCounterAddCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.delta = decodeLong(initialFrame.content, REQUEST_DELTA_FIELD_OFFSET);
        request.getBeforeUpdate = decodeBoolean(initialFrame.content, REQUEST_GET_BEFORE_UPDATE_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.replicaTimestamps = MapStringLongCodec.decode(iterator);
        request.targetReplica = AddressCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * TODO DOC
         */
        public long value;

        /**
         * last observed replica timestamps (vector clock)
         */
        public java.util.List<java.util.Map.Entry<java.lang.String, java.lang.Long>> replicaTimestamps;

        /**
         * TODO DOC
         */
        public int replicaCount;
    }

    public static ClientMessage encodeResponse(long value, java.util.Collection<java.util.Map.Entry<java.lang.String, java.lang.Long>> replicaTimestamps, int replicaCount) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        encodeLong(initialFrame.content, RESPONSE_VALUE_FIELD_OFFSET, value);
        encodeInt(initialFrame.content, RESPONSE_REPLICA_COUNT_FIELD_OFFSET, replicaCount);
        MapStringLongCodec.encode(clientMessage, replicaTimestamps);
        return clientMessage;
    }

    public static PNCounterAddCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.value = decodeLong(initialFrame.content, RESPONSE_VALUE_FIELD_OFFSET);
        response.replicaCount = decodeInt(initialFrame.content, RESPONSE_REPLICA_COUNT_FIELD_OFFSET);
        response.replicaTimestamps = MapStringLongCodec.decode(iterator);
        return response;
    }

}

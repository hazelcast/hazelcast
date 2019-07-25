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
import com.hazelcast.logging.Logger;

/**
 * TODO DOC
 */
public class ClientAddPartitionListenerCodec {

        private static final int REQUEST_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int REQUEST_MESSAGE_TYPE = 18;//hex: 0x0012,
        private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int RESPONSE_MESSAGE_TYPE = 100;//hex: 0x0064,
        private static final int EVENT_PARTITIONS_PARTITION_STATE_VERSION_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
        private static final int EVENT_PARTITIONS_INITIAL_FRAME_SIZE = EVENT_PARTITIONS_PARTITION_STATE_VERSION_FIELD_OFFSET + INT_SIZE_IN_BYTES;
        public static final int EVENT_PARTITIONS_MESSAGE_TYPE = 100;//hex: 0x00D9,

    public static class RequestParameters {
    }

    public static ClientMessage encodeRequest() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Client.AddPartitionListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        clientMessage.addFrame(initialFrame);
        return clientMessage;
    }

    public static ClientAddPartitionListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        RequestParameters request = new RequestParameters();
        iterator.next();//empty initial frame
        return request;
    }

    public static class ResponseParameters {
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.addFrame(initialFrame);

        return clientMessage;
    }

    public static ClientAddPartitionListenerCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        ResponseParameters response = new ResponseParameters();
        iterator.next();//empty initial frame
        return response;
    }

    public static ClientMessage encodePartitionsEvent(java.util.Collection<java.util.Map.Entry<com.hazelcast.nio.Address,java.util.List<java.lang.Integer>>> partitions, int partitionStateVersion) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_PARTITIONS_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_PARTITIONS_MESSAGE_TYPE);
        encodeInt(initialFrame.content, EVENT_PARTITIONS_PARTITION_STATE_VERSION_FIELD_OFFSET, partitionStateVersion);
        clientMessage.addFrame(initialFrame);
        MapCodec.encode(clientMessage, partitions, AddressCodec::encode, ListIntegerCodec::encode);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
            ClientMessage.Frame frame;
            if (messageType == EVENT_PARTITIONS_MESSAGE_TYPE) {
                frame = iterator.next();
                int partitionStateVersion = decodeInt(frame.content, EVENT_PARTITIONS_PARTITION_STATE_VERSION_FIELD_OFFSET);
                java.util.List<java.util.Map.Entry<com.hazelcast.nio.Address, java.util.List<java.lang.Integer>>> partitions = MapCodec.decode(iterator, AddressCodec::decode, ListIntegerCodec::decode);
                handlePartitionsEvent(partitions, partitionStateVersion);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }
        public abstract void handlePartitionsEvent(java.util.Collection<java.util.Map.Entry<com.hazelcast.nio.Address,java.util.List<java.lang.Integer>>> partitions, int partitionStateVersion);
    }
}
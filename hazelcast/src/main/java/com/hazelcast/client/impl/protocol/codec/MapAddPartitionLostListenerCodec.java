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
 * Adds a MapPartitionLostListener. The addPartitionLostListener returns a register-id. This id is needed to remove
 * the MapPartitionLostListener using the removePartitionLostListener(String) method.
 * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
 * IMPORTANT: Please see com.hazelcast.partition.PartitionLostListener for weaknesses.
 * IMPORTANT: Listeners registered from HazelcastClient may miss some of the map partition lost events due
 * to design limitations.
 */
public final class MapAddPartitionLostListenerCodec {
    //hex: 0x011F00
    public static final int REQUEST_MESSAGE_TYPE = 73472;
    //hex: 0x011F01
    public static final int RESPONSE_MESSAGE_TYPE = 73473;
    private static final int REQUEST_LOCAL_ONLY_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_LOCAL_ONLY_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int EVENT_MAP_PARTITION_LOST_PARTITION_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_MAP_PARTITION_LOST_INITIAL_FRAME_SIZE = EVENT_MAP_PARTITION_LOST_PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    //hex: 0x011F02
    private static final int EVENT_MAP_PARTITION_LOST_MESSAGE_TYPE = 73474;

    private MapAddPartitionLostListenerCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * name of map
         */
        public java.lang.String name;

        /**
         * if true fires events that originated from this node only, otherwise fires all events
         */
        public boolean localOnly;
    }

    public static ClientMessage encodeRequest(java.lang.String name, boolean localOnly) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Map.AddPartitionLostListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET, localOnly);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static MapAddPartitionLostListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.localOnly = decodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * returns the registration id for the MapPartitionLostListener.
         */
        public java.lang.String response;
    }

    public static ClientMessage encodeResponse(java.lang.String response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, response);
        return clientMessage;
    }

    public static MapAddPartitionLostListenerCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        response.response = StringCodec.decode(iterator);
        return response;
    }

    public static ClientMessage encodeMapPartitionLostEvent(int partitionId, java.lang.String uuid) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_MAP_PARTITION_LOST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_MAP_PARTITION_LOST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, EVENT_MAP_PARTITION_LOST_PARTITION_ID_FIELD_OFFSET, partitionId);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, uuid);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
            if (messageType == EVENT_MAP_PARTITION_LOST_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                int partitionId = decodeInt(initialFrame.content, EVENT_MAP_PARTITION_LOST_PARTITION_ID_FIELD_OFFSET);
                java.lang.String uuid = StringCodec.decode(iterator);
                handleMapPartitionLostEvent(partitionId, uuid);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }
        public abstract void handleMapPartitionLostEvent(int partitionId, java.lang.String uuid);
    }
}

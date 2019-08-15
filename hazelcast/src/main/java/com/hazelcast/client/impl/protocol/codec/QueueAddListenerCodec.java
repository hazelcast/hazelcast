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
 * Adds an listener for this collection. Listener will be notified or all collection add/remove events.
 */
public final class QueueAddListenerCodec {
    //hex: 0x031100
    public static final int REQUEST_MESSAGE_TYPE = 200960;
    //hex: 0x031101
    public static final int RESPONSE_MESSAGE_TYPE = 200961;
    private static final int REQUEST_INCLUDE_VALUE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_LOCAL_ONLY_FIELD_OFFSET = REQUEST_INCLUDE_VALUE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_LOCAL_ONLY_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int EVENT_ITEM_EVENT_TYPE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_ITEM_INITIAL_FRAME_SIZE = EVENT_ITEM_EVENT_TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    //hex: 0x031102
    private static final int EVENT_ITEM_MESSAGE_TYPE = 200962;

    private QueueAddListenerCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the Queue
         */
        public java.lang.String name;

        /**
         * <tt>true</tt> if the updated item should be passed to the item listener, <tt>false</tt> otherwise.
         */
        public boolean includeValue;

        /**
         * if true fires events that originated from this node only, otherwise fires all events
         */
        public boolean localOnly;
    }

    public static ClientMessage encodeRequest(java.lang.String name, boolean includeValue, boolean localOnly) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Queue.AddListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, REQUEST_INCLUDE_VALUE_FIELD_OFFSET, includeValue);
        encodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET, localOnly);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static QueueAddListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.includeValue = decodeBoolean(initialFrame.content, REQUEST_INCLUDE_VALUE_FIELD_OFFSET);
        request.localOnly = decodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * The registration id
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

    public static QueueAddListenerCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        response.response = StringCodec.decode(iterator);
        return response;
    }

    public static ClientMessage encodeItemEvent(com.hazelcast.nio.serialization.Data item, java.lang.String uuid, int eventType) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_ITEM_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_ITEM_MESSAGE_TYPE);
        encodeInt(initialFrame.content, EVENT_ITEM_EVENT_TYPE_FIELD_OFFSET, eventType);
        clientMessage.add(initialFrame);
        CodecUtil.encodeNullable(clientMessage, item, DataCodec::encode);
        StringCodec.encode(clientMessage, uuid);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
            if (messageType == EVENT_ITEM_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                int eventType = decodeInt(initialFrame.content, EVENT_ITEM_EVENT_TYPE_FIELD_OFFSET);
                com.hazelcast.nio.serialization.Data item = CodecUtil.decodeNullable(iterator, DataCodec::decode);
                java.lang.String uuid = StringCodec.decode(iterator);
                handleItemEvent(item, uuid, eventType);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }
        public abstract void handleItemEvent(com.hazelcast.nio.serialization.Data item, java.lang.String uuid, int eventType);
    }
}

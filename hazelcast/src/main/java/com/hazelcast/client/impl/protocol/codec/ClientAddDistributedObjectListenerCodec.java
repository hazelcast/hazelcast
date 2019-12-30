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
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;
import com.hazelcast.client.impl.protocol.codec.custom.*;
import com.hazelcast.logging.Logger;

import javax.annotation.Nullable;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/*
 * This file is auto-generated by the Hazelcast Client Protocol Code Generator.
 * To change this file, edit the templates or the protocol
 * definitions on the https://github.com/hazelcast/hazelcast-client-protocol
 * and regenerate it.
 */

/**
 * TODO DOC
 */
@Generated("144e5c3164540f34dd468dc838db14b6")
public final class ClientAddDistributedObjectListenerCodec {
    //hex: 0x000A00
    public static final int REQUEST_MESSAGE_TYPE = 2560;
    //hex: 0x000A01
    public static final int RESPONSE_MESSAGE_TYPE = 2561;
    private static final int REQUEST_LOCAL_ONLY_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INTERNAL_FIELD_OFFSET = REQUEST_LOCAL_ONLY_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_INTERNAL_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_DISTRIBUTED_OBJECT_SOURCE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_DISTRIBUTED_OBJECT_INITIAL_FRAME_SIZE = EVENT_DISTRIBUTED_OBJECT_SOURCE_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    //hex: 0x000A02
    private static final int EVENT_DISTRIBUTED_OBJECT_MESSAGE_TYPE = 2562;

    private ClientAddDistributedObjectListenerCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * If set to true, the server adds the listener only to itself, otherwise the listener is is added for all
         * members in the cluster.
         */
        public boolean localOnly;

        /**
         * Set to true for the registration for ProxyManager initiation and set to false for user listeners.
         */
        public boolean internal;
    }

    public static ClientMessage encodeRequest(boolean localOnly, boolean internal) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Client.AddDistributedObjectListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET, localOnly);
        encodeBoolean(initialFrame.content, REQUEST_INTERNAL_FIELD_OFFSET, internal);
        clientMessage.add(initialFrame);
        return clientMessage;
    }

    public static ClientAddDistributedObjectListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.localOnly = decodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET);
        request.internal = decodeBoolean(initialFrame.content, REQUEST_INTERNAL_FIELD_OFFSET);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * The registration id for the distributed object listener.
         */
        public java.util.UUID response;
    }

    public static ClientMessage encodeResponse(java.util.UUID response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeUUID(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static ClientAddDistributedObjectListenerCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.response = decodeUUID(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
        return response;
    }

    public static ClientMessage encodeDistributedObjectEvent(java.lang.String name, java.lang.String serviceName, java.lang.String eventType, java.util.UUID source) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_DISTRIBUTED_OBJECT_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_DISTRIBUTED_OBJECT_MESSAGE_TYPE);
        encodeUUID(initialFrame.content, EVENT_DISTRIBUTED_OBJECT_SOURCE_FIELD_OFFSET, source);
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, name);
        StringCodec.encode(clientMessage, serviceName);
        StringCodec.encode(clientMessage, eventType);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
            if (messageType == EVENT_DISTRIBUTED_OBJECT_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                java.util.UUID source = decodeUUID(initialFrame.content, EVENT_DISTRIBUTED_OBJECT_SOURCE_FIELD_OFFSET);
                java.lang.String name = StringCodec.decode(iterator);
                java.lang.String serviceName = StringCodec.decode(iterator);
                java.lang.String eventType = StringCodec.decode(iterator);
                handleDistributedObjectEvent(name, serviceName, eventType, source);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }
        public abstract void handleDistributedObjectEvent(java.lang.String name, java.lang.String serviceName, java.lang.String eventType, java.util.UUID source);
    }
}

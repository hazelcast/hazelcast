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
 * Adds listener to map. This listener will be used to listen near cache invalidation events.
 */
@Generated("3b324f72d288090cb57451223aebdeea")
public final class MapAddNearCacheInvalidationListenerCodec {
    //hex: 0x014100
    public static final int REQUEST_MESSAGE_TYPE = 82176;
    //hex: 0x014101
    public static final int RESPONSE_MESSAGE_TYPE = 82177;
    private static final int REQUEST_LISTENER_FLAGS_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_LOCAL_ONLY_FIELD_OFFSET = REQUEST_LISTENER_FLAGS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_LOCAL_ONLY_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_I_MAP_INVALIDATION_SOURCE_UUID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_I_MAP_INVALIDATION_PARTITION_UUID_FIELD_OFFSET = EVENT_I_MAP_INVALIDATION_SOURCE_UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_I_MAP_INVALIDATION_SEQUENCE_FIELD_OFFSET = EVENT_I_MAP_INVALIDATION_PARTITION_UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_I_MAP_INVALIDATION_INITIAL_FRAME_SIZE = EVENT_I_MAP_INVALIDATION_SEQUENCE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    //hex: 0x014102
    private static final int EVENT_I_MAP_INVALIDATION_MESSAGE_TYPE = 82178;
    private static final int EVENT_I_MAP_BATCH_INVALIDATION_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    //hex: 0x014103
    private static final int EVENT_I_MAP_BATCH_INVALIDATION_MESSAGE_TYPE = 82179;

    private MapAddNearCacheInvalidationListenerCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * name of the map
         */
        public java.lang.String name;

        /**
         * flags of enabled listeners.
         */
        public int listenerFlags;

        /**
         * if true fires events that originated from this node only, otherwise fires all events
         */
        public boolean localOnly;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int listenerFlags, boolean localOnly) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Map.AddNearCacheInvalidationListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, REQUEST_LISTENER_FLAGS_FIELD_OFFSET, listenerFlags);
        encodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET, localOnly);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static MapAddNearCacheInvalidationListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.listenerFlags = decodeInt(initialFrame.content, REQUEST_LISTENER_FLAGS_FIELD_OFFSET);
        request.localOnly = decodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * A unique string which is used as a key to remove the listener.
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

    public static MapAddNearCacheInvalidationListenerCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.response = decodeUUID(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
        return response;
    }

    public static ClientMessage encodeIMapInvalidationEvent(@Nullable com.hazelcast.nio.serialization.Data key, java.util.UUID sourceUuid, java.util.UUID partitionUuid, long sequence) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_I_MAP_INVALIDATION_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_I_MAP_INVALIDATION_MESSAGE_TYPE);
        encodeUUID(initialFrame.content, EVENT_I_MAP_INVALIDATION_SOURCE_UUID_FIELD_OFFSET, sourceUuid);
        encodeUUID(initialFrame.content, EVENT_I_MAP_INVALIDATION_PARTITION_UUID_FIELD_OFFSET, partitionUuid);
        encodeLong(initialFrame.content, EVENT_I_MAP_INVALIDATION_SEQUENCE_FIELD_OFFSET, sequence);
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, key, DataCodec::encode);
        return clientMessage;
    }
    public static ClientMessage encodeIMapBatchInvalidationEvent(java.util.Collection<com.hazelcast.nio.serialization.Data> keys, java.util.Collection<java.util.UUID> sourceUuids, java.util.Collection<java.util.UUID> partitionUuids, java.util.Collection<java.lang.Long> sequences) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_I_MAP_BATCH_INVALIDATION_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_I_MAP_BATCH_INVALIDATION_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        ListMultiFrameCodec.encode(clientMessage, keys, DataCodec::encode);
        ListUUIDCodec.encode(clientMessage, sourceUuids);
        ListUUIDCodec.encode(clientMessage, partitionUuids);
        ListLongCodec.encode(clientMessage, sequences);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
            if (messageType == EVENT_I_MAP_INVALIDATION_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                java.util.UUID sourceUuid = decodeUUID(initialFrame.content, EVENT_I_MAP_INVALIDATION_SOURCE_UUID_FIELD_OFFSET);
                java.util.UUID partitionUuid = decodeUUID(initialFrame.content, EVENT_I_MAP_INVALIDATION_PARTITION_UUID_FIELD_OFFSET);
                long sequence = decodeLong(initialFrame.content, EVENT_I_MAP_INVALIDATION_SEQUENCE_FIELD_OFFSET);
                com.hazelcast.nio.serialization.Data key = CodecUtil.decodeNullable(iterator, DataCodec::decode);
                handleIMapInvalidationEvent(key, sourceUuid, partitionUuid, sequence);
                return;
            }
            if (messageType == EVENT_I_MAP_BATCH_INVALIDATION_MESSAGE_TYPE) {
                //empty initial frame
                iterator.next();
                java.util.Collection<com.hazelcast.nio.serialization.Data> keys = ListMultiFrameCodec.decode(iterator, DataCodec::decode);
                java.util.Collection<java.util.UUID> sourceUuids = ListUUIDCodec.decode(iterator);
                java.util.Collection<java.util.UUID> partitionUuids = ListUUIDCodec.decode(iterator);
                java.util.Collection<java.lang.Long> sequences = ListLongCodec.decode(iterator);
                handleIMapBatchInvalidationEvent(keys, sourceUuids, partitionUuids, sequences);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }
        public abstract void handleIMapInvalidationEvent(@Nullable com.hazelcast.nio.serialization.Data key, java.util.UUID sourceUuid, java.util.UUID partitionUuid, long sequence);
        public abstract void handleIMapBatchInvalidationEvent(java.util.Collection<com.hazelcast.nio.serialization.Data> keys, java.util.Collection<java.util.UUID> sourceUuids, java.util.Collection<java.util.UUID> partitionUuids, java.util.Collection<java.lang.Long> sequences);
    }
}

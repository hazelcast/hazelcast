/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
 * Adds listener to cache. This listener will be used to listen near cache invalidation events.
 */
@Generated("727845f7496a0b41c572807d24ade6fa")
public final class CacheAddNearCacheInvalidationListenerCodec {
    //hex: 0x131D00
    public static final int REQUEST_MESSAGE_TYPE = 1252608;
    //hex: 0x131D01
    public static final int RESPONSE_MESSAGE_TYPE = 1252609;
    private static final int REQUEST_LOCAL_ONLY_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_LOCAL_ONLY_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_CACHE_INVALIDATION_SOURCE_UUID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_CACHE_INVALIDATION_PARTITION_UUID_FIELD_OFFSET = EVENT_CACHE_INVALIDATION_SOURCE_UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_CACHE_INVALIDATION_SEQUENCE_FIELD_OFFSET = EVENT_CACHE_INVALIDATION_PARTITION_UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_CACHE_INVALIDATION_INITIAL_FRAME_SIZE = EVENT_CACHE_INVALIDATION_SEQUENCE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    //hex: 0x131D02
    private static final int EVENT_CACHE_INVALIDATION_MESSAGE_TYPE = 1252610;
    private static final int EVENT_CACHE_BATCH_INVALIDATION_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    //hex: 0x131D03
    private static final int EVENT_CACHE_BATCH_INVALIDATION_MESSAGE_TYPE = 1252611;

    private CacheAddNearCacheInvalidationListenerCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the cache.
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
        clientMessage.setOperationName("Cache.AddNearCacheInvalidationListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET, localOnly);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static CacheAddNearCacheInvalidationListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.localOnly = decodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    public static ClientMessage encodeResponse(java.util.UUID response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeUUID(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    /**
     * Registration id for the registered listener.
     */
    public static java.util.UUID decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeUUID(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }

    public static ClientMessage encodeCacheInvalidationEvent(java.lang.String name, @Nullable com.hazelcast.internal.serialization.Data key, @Nullable java.util.UUID sourceUuid, java.util.UUID partitionUuid, long sequence) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_CACHE_INVALIDATION_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_CACHE_INVALIDATION_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeUUID(initialFrame.content, EVENT_CACHE_INVALIDATION_SOURCE_UUID_FIELD_OFFSET, sourceUuid);
        encodeUUID(initialFrame.content, EVENT_CACHE_INVALIDATION_PARTITION_UUID_FIELD_OFFSET, partitionUuid);
        encodeLong(initialFrame.content, EVENT_CACHE_INVALIDATION_SEQUENCE_FIELD_OFFSET, sequence);
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, name);
        CodecUtil.encodeNullable(clientMessage, key, DataCodec::encode);
        return clientMessage;
    }

    public static ClientMessage encodeCacheBatchInvalidationEvent(java.lang.String name, java.util.Collection<com.hazelcast.internal.serialization.Data> keys, java.util.Collection<java.util.UUID> sourceUuids, java.util.Collection<java.util.UUID> partitionUuids, java.util.Collection<java.lang.Long> sequences) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_CACHE_BATCH_INVALIDATION_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_CACHE_BATCH_INVALIDATION_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, name);
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
            if (messageType == EVENT_CACHE_INVALIDATION_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                java.util.UUID sourceUuid = decodeUUID(initialFrame.content, EVENT_CACHE_INVALIDATION_SOURCE_UUID_FIELD_OFFSET);
                java.util.UUID partitionUuid = decodeUUID(initialFrame.content, EVENT_CACHE_INVALIDATION_PARTITION_UUID_FIELD_OFFSET);
                long sequence = decodeLong(initialFrame.content, EVENT_CACHE_INVALIDATION_SEQUENCE_FIELD_OFFSET);
                java.lang.String name = StringCodec.decode(iterator);
                com.hazelcast.internal.serialization.Data key = CodecUtil.decodeNullable(iterator, DataCodec::decode);
                handleCacheInvalidationEvent(name, key, sourceUuid, partitionUuid, sequence);
                return;
            }
            if (messageType == EVENT_CACHE_BATCH_INVALIDATION_MESSAGE_TYPE) {
                //empty initial frame
                iterator.next();
                java.lang.String name = StringCodec.decode(iterator);
                java.util.Collection<com.hazelcast.internal.serialization.Data> keys = ListMultiFrameCodec.decode(iterator, DataCodec::decode);
                java.util.Collection<java.util.UUID> sourceUuids = ListUUIDCodec.decode(iterator);
                java.util.Collection<java.util.UUID> partitionUuids = ListUUIDCodec.decode(iterator);
                java.util.Collection<java.lang.Long> sequences = ListLongCodec.decode(iterator);
                handleCacheBatchInvalidationEvent(name, keys, sourceUuids, partitionUuids, sequences);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }

        /**
         * @param name Name of the cache.
         * @param key The key of the invalidated entry.
         * @param sourceUuid UUID of the member who fired this event.
         * @param partitionUuid UUID of the source partition that invalidated entry belongs to.
         * @param sequence Sequence number of the invalidation event.
         */
        public abstract void handleCacheInvalidationEvent(java.lang.String name, @Nullable com.hazelcast.internal.serialization.Data key, @Nullable java.util.UUID sourceUuid, java.util.UUID partitionUuid, long sequence);

        /**
         * @param name Name of the cache.
         * @param keys List of the keys of the invalidated entries.
         * @param sourceUuids List of UUIDs of the members who fired these events.
         * @param partitionUuids List of UUIDs of the source partitions that invalidated entries belong to.
         * @param sequences List of sequence numbers of the invalidation events.
         */
        public abstract void handleCacheBatchInvalidationEvent(java.lang.String name, java.util.Collection<com.hazelcast.internal.serialization.Data> keys, java.util.Collection<java.util.UUID> sourceUuids, java.util.Collection<java.util.UUID> partitionUuids, java.util.Collection<java.lang.Long> sequences);
    }
}

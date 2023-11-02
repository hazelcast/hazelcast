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
 * Adds a near cache entry listener for this map. This listener will be notified when an entry is added/removed/updated/evicted/expired etc. so that the near cache entries can be invalidated.
 */
@SuppressWarnings("unused")
@Generated("23b60825eaa2dfdfe75c24dc1399a39d")
public final class ReplicatedMapAddNearCacheEntryListenerCodec {
    //hex: 0x0D1200
    public static final int REQUEST_MESSAGE_TYPE = 856576;
    //hex: 0x0D1201
    public static final int RESPONSE_MESSAGE_TYPE = 856577;
    private static final int REQUEST_INCLUDE_VALUE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_LOCAL_ONLY_FIELD_OFFSET = REQUEST_INCLUDE_VALUE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_LOCAL_ONLY_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_ENTRY_EVENT_TYPE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_ENTRY_UUID_FIELD_OFFSET = EVENT_ENTRY_EVENT_TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_FIELD_OFFSET = EVENT_ENTRY_UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_ENTRY_INITIAL_FRAME_SIZE = EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    //hex: 0x0D1202
    private static final int EVENT_ENTRY_MESSAGE_TYPE = 856578;

    private ReplicatedMapAddNearCacheEntryListenerCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the ReplicatedMap
         */
        public java.lang.String name;

        /**
         * True if EntryEvent should contain the value,false otherwise
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
        clientMessage.setOperationName("ReplicatedMap.AddNearCacheEntryListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeBoolean(initialFrame.content, REQUEST_INCLUDE_VALUE_FIELD_OFFSET, includeValue);
        encodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET, localOnly);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static ReplicatedMapAddNearCacheEntryListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.includeValue = decodeBoolean(initialFrame.content, REQUEST_INCLUDE_VALUE_FIELD_OFFSET);
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
     * A unique string  which is used as a key to remove the listener.
     */
    public static java.util.UUID decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeUUID(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }

    public static ClientMessage encodeEntryEvent(@Nullable com.hazelcast.internal.serialization.Data key, @Nullable com.hazelcast.internal.serialization.Data value, @Nullable com.hazelcast.internal.serialization.Data oldValue, @Nullable com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_ENTRY_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_ENTRY_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, EVENT_ENTRY_EVENT_TYPE_FIELD_OFFSET, eventType);
        encodeUUID(initialFrame.content, EVENT_ENTRY_UUID_FIELD_OFFSET, uuid);
        encodeInt(initialFrame.content, EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_FIELD_OFFSET, numberOfAffectedEntries);
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, key, DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, value, DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, oldValue, DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, mergingValue, DataCodec::encode);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
            if (messageType == EVENT_ENTRY_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                int eventType = decodeInt(initialFrame.content, EVENT_ENTRY_EVENT_TYPE_FIELD_OFFSET);
                java.util.UUID uuid = decodeUUID(initialFrame.content, EVENT_ENTRY_UUID_FIELD_OFFSET);
                int numberOfAffectedEntries = decodeInt(initialFrame.content, EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_FIELD_OFFSET);
                com.hazelcast.internal.serialization.Data key = CodecUtil.decodeNullable(iterator, DataCodec::decode);
                com.hazelcast.internal.serialization.Data value = CodecUtil.decodeNullable(iterator, DataCodec::decode);
                com.hazelcast.internal.serialization.Data oldValue = CodecUtil.decodeNullable(iterator, DataCodec::decode);
                com.hazelcast.internal.serialization.Data mergingValue = CodecUtil.decodeNullable(iterator, DataCodec::decode);
                handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }

        /**
         * @param key Key of the entry event.
         * @param value Value of the entry event.
         * @param oldValue Old value of the entry event.
         * @param mergingValue Incoming merging value of the entry event.
         * @param eventType Type of the entry event. Possible values are
         *                  ADDED(1)
         *                  REMOVED(2)
         *                  UPDATED(4)
         *                  EVICTED(8)
         *                  EXPIRED(16)
         *                  EVICT_ALL(32)
         *                  CLEAR_ALL(64)
         *                  MERGED(128)
         *                  INVALIDATION(256)
         *                  LOADED(512)
         * @param uuid UUID of the member that dispatches the event.
         * @param numberOfAffectedEntries Number of entries affected by this event.
         */
        public abstract void handleEntryEvent(@Nullable com.hazelcast.internal.serialization.Data key, @Nullable com.hazelcast.internal.serialization.Data value, @Nullable com.hazelcast.internal.serialization.Data oldValue, @Nullable com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries);
    }
}

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
 * Adds a new event journal configuration to a running cluster.
 * If an event journal configuration for the same map or cache name already exists, then
 * the new configuration is ignored and the existing one is preserved.
 */
public class DynamicConfigAddEventJournalConfigCodec {

        private static final int REQUEST_ENABLED_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
        private static final int REQUEST_CAPACITY_FIELD_OFFSET = REQUEST_ENABLED_FIELD_OFFSET + INT_SIZE_IN_BYTES;
        private static final int REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET = REQUEST_CAPACITY_FIELD_OFFSET + INT_SIZE_IN_BYTES;
        private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int REQUEST_MESSAGE_TYPE = 7697;//hex: 0x1E11,
        private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int RESPONSE_MESSAGE_TYPE = 100;//hex: 0x0064,

    public static class RequestParameters {

        /**
         * name of {@code IMap} to use as event source
         */
        public java.lang.String mapName;

        /**
         * name of {@code ICache} to use as event source
         */
        public java.lang.String cacheName;

        /**
         * {@code true} to enable this event journal configuration, otherwise {@code false}
         */
        public boolean enabled;

        /**
         * capacity of event journal
         */
        public int capacity;

        /**
         * time to live (in seconds). This is the time the event journal retains items before removing them
         * from the journal.
         */
        public int timeToLiveSeconds;
    }

    public static ClientMessage encodeRequest(java.lang.String mapName, java.lang.String cacheName, boolean enabled, int capacity, int timeToLiveSeconds) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("DynamicConfig.AddEventJournalConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, REQUEST_ENABLED_FIELD_OFFSET, enabled);
        encodeInt(initialFrame.content, REQUEST_CAPACITY_FIELD_OFFSET, capacity);
        encodeInt(initialFrame.content, REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET, timeToLiveSeconds);
        clientMessage.addFrame(initialFrame);
        CodecUtil.encodeNullable(clientMessage, mapName, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheName, StringCodec::encode);
        return clientMessage;
    }

    public static DynamicConfigAddEventJournalConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.enabled = decodeBoolean(initialFrame.content, REQUEST_ENABLED_FIELD_OFFSET);
        request.capacity = decodeInt(initialFrame.content, REQUEST_CAPACITY_FIELD_OFFSET);
        request.timeToLiveSeconds = decodeInt(initialFrame.content, REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET);
        request.mapName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.cacheName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
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

    public static DynamicConfigAddEventJournalConfigCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        ResponseParameters response = new ResponseParameters();
        iterator.next();//empty initial frame
        return response;
    }

}
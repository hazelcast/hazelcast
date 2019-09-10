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
public final class ContinuousQueryAddListenerCodec {
    //hex: 0x180400
    public static final int REQUEST_MESSAGE_TYPE = 1573888;
    //hex: 0x180401
    public static final int RESPONSE_MESSAGE_TYPE = 1573889;
    private static final int REQUEST_LOCAL_ONLY_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_LOCAL_ONLY_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int EVENT_QUERY_CACHE_SINGLE_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    //hex: 0x180402
    private static final int EVENT_QUERY_CACHE_SINGLE_MESSAGE_TYPE = 1573890;
    private static final int EVENT_QUERY_CACHE_BATCH_PARTITION_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_QUERY_CACHE_BATCH_INITIAL_FRAME_SIZE = EVENT_QUERY_CACHE_BATCH_PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    //hex: 0x180403
    private static final int EVENT_QUERY_CACHE_BATCH_MESSAGE_TYPE = 1573891;

    private ContinuousQueryAddListenerCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the MapListener which will be used to listen this QueryCache
         */
        public java.lang.String listenerName;

        /**
         * if true fires events that originated from this node only, otherwise fires all events
         */
        public boolean localOnly;
    }

    public static ClientMessage encodeRequest(java.lang.String listenerName, boolean localOnly) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("ContinuousQuery.AddListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET, localOnly);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, listenerName);
        return clientMessage;
    }

    public static ContinuousQueryAddListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.localOnly = decodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET);
        request.listenerName = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * Registration id for the listener.
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

    public static ContinuousQueryAddListenerCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        response.response = StringCodec.decode(iterator);
        return response;
    }

    public static ClientMessage encodeQueryCacheSingleEvent(com.hazelcast.map.impl.querycache.event.QueryCacheEventData data) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_QUERY_CACHE_SINGLE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_QUERY_CACHE_SINGLE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);
        QueryCacheEventDataCodec.encode(clientMessage, data);
        return clientMessage;
    }
    public static ClientMessage encodeQueryCacheBatchEvent(java.util.Collection<com.hazelcast.map.impl.querycache.event.QueryCacheEventData> events, java.lang.String source, int partitionId) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_QUERY_CACHE_BATCH_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_QUERY_CACHE_BATCH_MESSAGE_TYPE);
        encodeInt(initialFrame.content, EVENT_QUERY_CACHE_BATCH_PARTITION_ID_FIELD_OFFSET, partitionId);
        clientMessage.add(initialFrame);
        ListMultiFrameCodec.encode(clientMessage, events, QueryCacheEventDataCodec::encode);
        StringCodec.encode(clientMessage, source);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
            if (messageType == EVENT_QUERY_CACHE_SINGLE_MESSAGE_TYPE) {
                //empty initial frame
                iterator.next();
                com.hazelcast.map.impl.querycache.event.QueryCacheEventData data = QueryCacheEventDataCodec.decode(iterator);
                handleQueryCacheSingleEvent(data);
                return;
            }
            if (messageType == EVENT_QUERY_CACHE_BATCH_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                int partitionId = decodeInt(initialFrame.content, EVENT_QUERY_CACHE_BATCH_PARTITION_ID_FIELD_OFFSET);
                java.util.List<com.hazelcast.map.impl.querycache.event.QueryCacheEventData> events = ListMultiFrameCodec.decode(iterator, QueryCacheEventDataCodec::decode);
                java.lang.String source = StringCodec.decode(iterator);
                handleQueryCacheBatchEvent(events, source, partitionId);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }
        public abstract void handleQueryCacheSingleEvent(com.hazelcast.map.impl.querycache.event.QueryCacheEventData data);
        public abstract void handleQueryCacheBatchEvent(java.util.Collection<com.hazelcast.map.impl.querycache.event.QueryCacheEventData> events, java.lang.String source, int partitionId);
    }
}

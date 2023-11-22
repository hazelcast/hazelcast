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

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/*
 * This file is auto-generated by the Hazelcast Client Protocol Code Generator.
 * To change this file, edit the templates or the protocol
 * definitions on the https://github.com/hazelcast/hazelcast-client-protocol
 * and regenerate it.
 */

/**
 * Creates a publisher that does not include value for the cache events it sends.
 */
@SuppressWarnings("unused")
@Generated("30a5bdbfb75776e60043f1ec30ae5413")
public final class ContinuousQueryPublisherCreateCodec {
    //hex: 0x160200
    public static final int REQUEST_MESSAGE_TYPE = 1442304;
    //hex: 0x160201
    public static final int RESPONSE_MESSAGE_TYPE = 1442305;
    private static final int REQUEST_BATCH_SIZE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_BUFFER_SIZE_FIELD_OFFSET = REQUEST_BATCH_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_DELAY_SECONDS_FIELD_OFFSET = REQUEST_BUFFER_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_POPULATE_FIELD_OFFSET = REQUEST_DELAY_SECONDS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_COALESCE_FIELD_OFFSET = REQUEST_POPULATE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_COALESCE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private ContinuousQueryPublisherCreateCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the map.
         */
        public java.lang.String mapName;

        /**
         * Name of query cache.
         */
        public java.lang.String cacheName;

        /**
         * The predicate to filter events which will be applied to the QueryCache.
         */
        public com.hazelcast.internal.serialization.Data predicate;

        /**
         * The size of batch. After reaching this minimum size, node immediately sends buffered events to QueryCache.
         */
        public int batchSize;

        /**
         * Maximum number of events which can be stored in a buffer of partition.
         */
        public int bufferSize;

        /**
         * The minimum number of delay seconds which an event waits in the buffer of node.
         */
        public long delaySeconds;

        /**
         * Flag to enable/disable initial population of the QueryCache.
         */
        public boolean populate;

        /**
         * Flag to enable/disable coalescing. If true, then only the last updated value for a key is placed in the
         * batch, otherwise all changed values are included in the update.
         */
        public boolean coalesce;
    }

    public static ClientMessage encodeRequest(java.lang.String mapName, java.lang.String cacheName, com.hazelcast.internal.serialization.Data predicate, int batchSize, int bufferSize, long delaySeconds, boolean populate, boolean coalesce) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setContainsSerializedDataInRequest(true);
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("ContinuousQuery.PublisherCreate");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, REQUEST_BATCH_SIZE_FIELD_OFFSET, batchSize);
        encodeInt(initialFrame.content, REQUEST_BUFFER_SIZE_FIELD_OFFSET, bufferSize);
        encodeLong(initialFrame.content, REQUEST_DELAY_SECONDS_FIELD_OFFSET, delaySeconds);
        encodeBoolean(initialFrame.content, REQUEST_POPULATE_FIELD_OFFSET, populate);
        encodeBoolean(initialFrame.content, REQUEST_COALESCE_FIELD_OFFSET, coalesce);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, mapName);
        StringCodec.encode(clientMessage, cacheName);
        DataCodec.encode(clientMessage, predicate);
        return clientMessage;
    }

    public static ContinuousQueryPublisherCreateCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.batchSize = decodeInt(initialFrame.content, REQUEST_BATCH_SIZE_FIELD_OFFSET);
        request.bufferSize = decodeInt(initialFrame.content, REQUEST_BUFFER_SIZE_FIELD_OFFSET);
        request.delaySeconds = decodeLong(initialFrame.content, REQUEST_DELAY_SECONDS_FIELD_OFFSET);
        request.populate = decodeBoolean(initialFrame.content, REQUEST_POPULATE_FIELD_OFFSET);
        request.coalesce = decodeBoolean(initialFrame.content, REQUEST_COALESCE_FIELD_OFFSET);
        request.mapName = StringCodec.decode(iterator);
        request.cacheName = StringCodec.decode(iterator);
        request.predicate = DataCodec.decode(iterator);
        return request;
    }

    public static ClientMessage encodeResponse(java.util.Collection<com.hazelcast.internal.serialization.Data> response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        ListMultiFrameCodec.encode(clientMessage, response, DataCodec::encode);
        return clientMessage;
    }

    /**
     * Array of keys.
     */
    public static java.util.List<com.hazelcast.internal.serialization.Data> decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        //empty initial frame
        iterator.next();
        return ListMultiFrameCodec.decode(iterator, DataCodec::decode);
    }
}

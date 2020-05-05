/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
 * Starts execution of an SQL query.
 */
@Generated("28884598c3fdacc0c86328ee1dea4ced")
public final class SqlQueryExecuteCodec {
    //hex: 0x210100
    public static final int REQUEST_MESSAGE_TYPE = 2162944;
    //hex: 0x210101
    public static final int RESPONSE_MESSAGE_TYPE = 2162945;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_COLUMN_COUNT_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_COLUMN_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private SqlQueryExecuteCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Query to be executed.
         */
        public java.lang.String query;

        /**
         * Query parameters.
         */
        public @Nullable java.util.List<com.hazelcast.internal.serialization.Data> parameters;
    }

    public static ClientMessage encodeRequest(java.lang.String query, @Nullable java.util.Collection<com.hazelcast.internal.serialization.Data> parameters) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Sql.QueryExecute");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, query);
        ListMultiFrameCodec.encodeNullable(clientMessage, parameters, DataCodec::encode);
        return clientMessage;
    }

    public static SqlQueryExecuteCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        //empty initial frame
        iterator.next();
        request.query = StringCodec.decode(iterator);
        request.parameters = ListMultiFrameCodec.decodeNullable(iterator, DataCodec::decode);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * ID of the query which was started.
         */
        public com.hazelcast.internal.serialization.Data queryId;

        /**
         * Number of columns in the result.
         */
        public int columnCount;
    }

    public static ClientMessage encodeResponse(com.hazelcast.internal.serialization.Data queryId, int columnCount) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeInt(initialFrame.content, RESPONSE_COLUMN_COUNT_FIELD_OFFSET, columnCount);
        clientMessage.add(initialFrame);

        DataCodec.encode(clientMessage, queryId);
        return clientMessage;
    }

    public static SqlQueryExecuteCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.columnCount = decodeInt(initialFrame.content, RESPONSE_COLUMN_COUNT_FIELD_OFFSET);
        response.queryId = DataCodec.decode(iterator);
        return response;
    }
}

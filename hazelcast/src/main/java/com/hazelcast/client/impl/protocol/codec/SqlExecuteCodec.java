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
import com.hazelcast.client.impl.protocol.codec.custom.*;

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
@Generated("2fb2fb6fb01383f480d9ebb91b076438")
public final class SqlExecuteCodec {
    //hex: 0x210100
    public static final int REQUEST_MESSAGE_TYPE = 2162944;
    //hex: 0x210101
    public static final int RESPONSE_MESSAGE_TYPE = 2162945;
    private static final int REQUEST_TIMEOUT_MILLIS_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_CURSOR_BUFFER_SIZE_FIELD_OFFSET = REQUEST_TIMEOUT_MILLIS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_CURSOR_BUFFER_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_ROW_PAGE_LAST_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_ROW_PAGE_LAST_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private SqlExecuteCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Query string.
         */
        public java.lang.String sql;

        /**
         * Query parameters.
         */
        public java.util.List<com.hazelcast.internal.serialization.Data> parameters;

        /**
         * Timeout in milliseconds.
         */
        public long timeoutMillis;

        /**
         * Cursor buffer size.
         */
        public int cursorBufferSize;
    }

    public static ClientMessage encodeRequest(java.lang.String sql, java.util.Collection<com.hazelcast.internal.serialization.Data> parameters, long timeoutMillis, int cursorBufferSize) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Sql.Execute");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_TIMEOUT_MILLIS_FIELD_OFFSET, timeoutMillis);
        encodeInt(initialFrame.content, REQUEST_CURSOR_BUFFER_SIZE_FIELD_OFFSET, cursorBufferSize);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, sql);
        ListMultiFrameCodec.encode(clientMessage, parameters, DataCodec::encode);
        return clientMessage;
    }

    public static SqlExecuteCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.timeoutMillis = decodeLong(initialFrame.content, REQUEST_TIMEOUT_MILLIS_FIELD_OFFSET);
        request.cursorBufferSize = decodeInt(initialFrame.content, REQUEST_CURSOR_BUFFER_SIZE_FIELD_OFFSET);
        request.sql = StringCodec.decode(iterator);
        request.parameters = ListMultiFrameCodec.decode(iterator, DataCodec::decode);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {
        /**
         * Query ID.
         */
        public @Nullable java.lang.String queryId;
        /**
         * Row metadata.
         */
        public @Nullable java.util.List<com.hazelcast.sql.SqlColumnMetadata> rowMetadata;
        /**
         * Row page.
         */
        public @Nullable java.util.List<java.util.List<com.hazelcast.internal.serialization.Data>> rowPage;
        /**
         * Whether the row page is the last.
         */
        public boolean rowPageLast;
        /**
         * Error object.
         */
        public @Nullable com.hazelcast.sql.impl.client.SqlError error;
    }
    public static ClientMessage encodeResponse(@Nullable java.lang.String queryId, @Nullable java.util.List<com.hazelcast.sql.SqlColumnMetadata> rowMetadata, @Nullable java.util.Collection<java.util.Collection<com.hazelcast.internal.serialization.Data>> rowPage, boolean rowPageLast, @Nullable com.hazelcast.sql.impl.client.SqlError error) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, RESPONSE_ROW_PAGE_LAST_FIELD_OFFSET, rowPageLast);
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, queryId, StringCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, rowMetadata, SqlColumnMetadataCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, rowPage, ListDataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, error, SqlErrorCodec::encode);
        return clientMessage;
    }

    public static SqlExecuteCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.rowPageLast = decodeBoolean(initialFrame.content, RESPONSE_ROW_PAGE_LAST_FIELD_OFFSET);
        response.queryId = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        response.rowMetadata = ListMultiFrameCodec.decodeNullable(iterator, SqlColumnMetadataCodec::decode);
        response.rowPage = ListMultiFrameCodec.decodeNullable(iterator, ListDataCodec::decode);
        response.error = CodecUtil.decodeNullable(iterator, SqlErrorCodec::decode);
        return response;
    }

}

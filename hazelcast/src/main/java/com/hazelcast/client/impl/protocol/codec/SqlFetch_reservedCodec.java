/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
 * Fetches the next row page.
 */
@Generated("06d3b4944a0085ce85c26fcb01bd1430")
public final class SqlFetch_reservedCodec {
    //hex: 0x210200
    public static final int REQUEST_MESSAGE_TYPE = 2163200;
    //hex: 0x210201
    public static final int RESPONSE_MESSAGE_TYPE = 2163201;
    private static final int REQUEST_CURSOR_BUFFER_SIZE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_CURSOR_BUFFER_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_ROW_PAGE_LAST_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_ROW_PAGE_LAST_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private SqlFetch_reservedCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Query ID.
         */
        public com.hazelcast.sql.impl.QueryId queryId;

        /**
         * Cursor buffer size.
         */
        public int cursorBufferSize;
    }

    public static ClientMessage encodeRequest(com.hazelcast.sql.impl.QueryId queryId, int cursorBufferSize) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Sql.Fetch_reserved");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, REQUEST_CURSOR_BUFFER_SIZE_FIELD_OFFSET, cursorBufferSize);
        clientMessage.add(initialFrame);
        SqlQueryIdCodec.encode(clientMessage, queryId);
        return clientMessage;
    }

    public static SqlFetch_reservedCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.cursorBufferSize = decodeInt(initialFrame.content, REQUEST_CURSOR_BUFFER_SIZE_FIELD_OFFSET);
        request.queryId = SqlQueryIdCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

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

    public static ClientMessage encodeResponse(@Nullable java.util.Collection<java.util.Collection<com.hazelcast.internal.serialization.Data>> rowPage, boolean rowPageLast, @Nullable com.hazelcast.sql.impl.client.SqlError error) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, RESPONSE_ROW_PAGE_LAST_FIELD_OFFSET, rowPageLast);
        clientMessage.add(initialFrame);

        ListMultiFrameCodec.encodeNullable(clientMessage, rowPage, ListCNDataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, error, SqlErrorCodec::encode);
        return clientMessage;
    }

    public static SqlFetch_reservedCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.rowPageLast = decodeBoolean(initialFrame.content, RESPONSE_ROW_PAGE_LAST_FIELD_OFFSET);
        response.rowPage = ListMultiFrameCodec.decodeNullable(iterator, ListCNDataCodec::decode);
        response.error = CodecUtil.decodeNullable(iterator, SqlErrorCodec::decode);
        return response;
    }
}

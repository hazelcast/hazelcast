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
 * TODO DOC
 */
public final class TransactionCreateCodec {
    //hex: 0x170200
    public static final int REQUEST_MESSAGE_TYPE = 1507840;
    //hex: 0x170201
    public static final int RESPONSE_MESSAGE_TYPE = 1507841;
    private static final int REQUEST_TIMEOUT_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_DURABILITY_FIELD_OFFSET = REQUEST_TIMEOUT_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_TRANSACTION_TYPE_FIELD_OFFSET = REQUEST_DURABILITY_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_THREAD_ID_FIELD_OFFSET = REQUEST_TRANSACTION_TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_THREAD_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private TransactionCreateCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * The maximum allowed duration for the transaction operations.
         */
        public long timeout;

        /**
         * The durability of the transaction
         */
        public int durability;

        /**
         * Identifies the type of the transaction. Possible values are:
         * 1 (Two phase):  The two phase commit is more than the classic two phase commit (if you want a regular
         * two phase commit, use local). Before it commits, it copies the commit-log to other members, so in
         * case of member failure, another member can complete the commit.
         * 2 (Local): Unlike the name suggests, local is a two phase commit. So first all cohorts are asked
         * to prepare if everyone agrees then all cohorts are asked to commit. The problem happens when during
         * the commit phase one or more members crash, that the system could be left in an inconsistent state.
         */
        public int transactionType;

        /**
         * The thread id for the transaction.
         */
        public long threadId;
    }

    public static ClientMessage encodeRequest(long timeout, int durability, int transactionType, long threadId) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(true);
        clientMessage.setOperationName("Transaction.Create");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeLong(initialFrame.content, REQUEST_TIMEOUT_FIELD_OFFSET, timeout);
        encodeInt(initialFrame.content, REQUEST_DURABILITY_FIELD_OFFSET, durability);
        encodeInt(initialFrame.content, REQUEST_TRANSACTION_TYPE_FIELD_OFFSET, transactionType);
        encodeLong(initialFrame.content, REQUEST_THREAD_ID_FIELD_OFFSET, threadId);
        clientMessage.add(initialFrame);
        return clientMessage;
    }

    public static TransactionCreateCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.timeout = decodeLong(initialFrame.content, REQUEST_TIMEOUT_FIELD_OFFSET);
        request.durability = decodeInt(initialFrame.content, REQUEST_DURABILITY_FIELD_OFFSET);
        request.transactionType = decodeInt(initialFrame.content, REQUEST_TRANSACTION_TYPE_FIELD_OFFSET);
        request.threadId = decodeLong(initialFrame.content, REQUEST_THREAD_ID_FIELD_OFFSET);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * The transaction id for the created transaction.
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

    public static TransactionCreateCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        response.response = StringCodec.decode(iterator);
        return response;
    }

}

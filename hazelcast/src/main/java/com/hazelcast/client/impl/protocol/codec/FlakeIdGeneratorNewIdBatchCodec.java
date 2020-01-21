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
 * Fetches a new batch of ids for the given flake id generator.
 */
@Generated("37aff7a01e30d4b3e73c0a29c655245a")
public final class FlakeIdGeneratorNewIdBatchCodec {
    //hex: 0x1C0100
    public static final int REQUEST_MESSAGE_TYPE = 1835264;
    //hex: 0x1C0101
    public static final int RESPONSE_MESSAGE_TYPE = 1835265;
    private static final int REQUEST_BATCH_SIZE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_BATCH_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_BASE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INCREMENT_FIELD_OFFSET = RESPONSE_BASE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_BATCH_SIZE_FIELD_OFFSET = RESPONSE_INCREMENT_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BATCH_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private FlakeIdGeneratorNewIdBatchCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the flake id generator.
         */
        public java.lang.String name;

        /**
         * Number of ids that will be fetched on one call.
         */
        public int batchSize;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int batchSize) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("FlakeIdGenerator.NewIdBatch");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, REQUEST_BATCH_SIZE_FIELD_OFFSET, batchSize);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static FlakeIdGeneratorNewIdBatchCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.batchSize = decodeInt(initialFrame.content, REQUEST_BATCH_SIZE_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * First id in the batch.
         */
        public long base;

        /**
         * Increment for the next id in the batch.
         */
        public long increment;

        /**
         * Number of ids in the batch.
         */
        public int batchSize;
    }

    public static ClientMessage encodeResponse(long base, long increment, int batchSize) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeLong(initialFrame.content, RESPONSE_BASE_FIELD_OFFSET, base);
        encodeLong(initialFrame.content, RESPONSE_INCREMENT_FIELD_OFFSET, increment);
        encodeInt(initialFrame.content, RESPONSE_BATCH_SIZE_FIELD_OFFSET, batchSize);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static FlakeIdGeneratorNewIdBatchCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.base = decodeLong(initialFrame.content, RESPONSE_BASE_FIELD_OFFSET);
        response.increment = decodeLong(initialFrame.content, RESPONSE_INCREMENT_FIELD_OFFSET);
        response.batchSize = decodeInt(initialFrame.content, RESPONSE_BATCH_SIZE_FIELD_OFFSET);
        return response;
    }

}

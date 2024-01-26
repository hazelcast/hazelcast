/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
 * Adds all the items of a collection to the tail of the Ringbuffer. A addAll is likely to outperform multiple calls
 * to add(Object) due to better io utilization and a reduced number of executed operations. If the batch is empty,
 * the call is ignored. When the collection is not empty, the content is copied into a different data-structure.
 * This means that: after this call completes, the collection can be re-used. the collection doesn't need to be serializable.
 * If the collection is larger than the capacity of the ringbuffer, then the items that were written first will be
 * overwritten. Therefor this call will not block. The items are inserted in the order of the Iterator of the collection.
 * If an addAll is executed concurrently with an add or addAll, no guarantee is given that items are contiguous.
 * The result of the future contains the sequenceId of the last written item
 */
@SuppressWarnings("unused")
@Generated("d8b1e1cb41307866ab0e3d3f635693b9")
public final class RingbufferAddAllCodec {
    //hex: 0x170800
    public static final int REQUEST_MESSAGE_TYPE = 1509376;
    //hex: 0x170801
    public static final int RESPONSE_MESSAGE_TYPE = 1509377;
    private static final int REQUEST_OVERFLOW_POLICY_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_OVERFLOW_POLICY_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private RingbufferAddAllCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the Ringbuffer
         */
        public java.lang.String name;

        /**
         * the batch of items to add
         */
        public java.util.List<com.hazelcast.internal.serialization.Data> valueList;

        /**
         * the overflowPolicy to use
         */
        public int overflowPolicy;
    }

    public static ClientMessage encodeRequest(java.lang.String name, java.util.Collection<com.hazelcast.internal.serialization.Data> valueList, int overflowPolicy) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setContainsSerializedDataInRequest(true);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Ringbuffer.AddAll");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, REQUEST_OVERFLOW_POLICY_FIELD_OFFSET, overflowPolicy);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        ListMultiFrameCodec.encode(clientMessage, valueList, DataCodec::encode);
        return clientMessage;
    }

    public static RingbufferAddAllCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.overflowPolicy = decodeInt(initialFrame.content, REQUEST_OVERFLOW_POLICY_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.valueList = ListMultiFrameCodec.decode(iterator, DataCodec::decode);
        return request;
    }

    public static ClientMessage encodeResponse(long response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeLong(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    /**
     * the CompletionStage to synchronize on completion.
     */
    public static long decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeLong(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }
}

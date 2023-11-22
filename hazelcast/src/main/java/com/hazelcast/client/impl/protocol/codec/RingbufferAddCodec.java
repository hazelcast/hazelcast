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
 * Adds an item to the tail of the Ringbuffer. If there is space in the ringbuffer, the call
 * will return the sequence of the written item. If there is no space, it depends on the overflow policy what happens:
 * OverflowPolicy OVERWRITE we just overwrite the oldest item in the ringbuffer and we violate the ttl
 * OverflowPolicy FAIL we return -1. The reason that FAIL exist is to give the opportunity to obey the ttl.
 * <p/>
 * This sequence will always be unique for this Ringbuffer instance so it can be used as a unique id generator if you are
 * publishing items on this Ringbuffer. However you need to take care of correctly determining an initial id when any node
 * uses the ringbuffer for the first time. The most reliable way to do that is to write a dummy item into the ringbuffer and
 * use the returned sequence as initial  id. On the reading side, this dummy item should be discard. Please keep in mind that
 * this id is not the sequence of the item you are about to publish but from a previously published item. So it can't be used
 * to find that item.
 */
@SuppressWarnings("unused")
@Generated("37c184118eb34d9dd282c4a9705c1434")
public final class RingbufferAddCodec {
    //hex: 0x170600
    public static final int REQUEST_MESSAGE_TYPE = 1508864;
    //hex: 0x170601
    public static final int RESPONSE_MESSAGE_TYPE = 1508865;
    private static final int REQUEST_OVERFLOW_POLICY_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_OVERFLOW_POLICY_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private RingbufferAddCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the Ringbuffer
         */
        public java.lang.String name;

        /**
         * the OverflowPolicy to use.
         */
        public int overflowPolicy;

        /**
         * to item to add
         */
        public com.hazelcast.internal.serialization.Data value;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int overflowPolicy, com.hazelcast.internal.serialization.Data value) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setContainsSerializedDataInRequest(true);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Ringbuffer.Add");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, REQUEST_OVERFLOW_POLICY_FIELD_OFFSET, overflowPolicy);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        DataCodec.encode(clientMessage, value);
        return clientMessage;
    }

    public static RingbufferAddCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.overflowPolicy = decodeInt(initialFrame.content, REQUEST_OVERFLOW_POLICY_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.value = DataCodec.decode(iterator);
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
     * the sequence of the added item, or -1 if the add failed.
     */
    public static long decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeLong(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }
}

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
 * Puts the key-value into the specified map.
 */
@SuppressWarnings("unused")
@Generated("ec8ee0e655558920020d9ad7670d31f0")
public final class CPMapPutCodec {
    //hex: 0x230200
    public static final int REQUEST_MESSAGE_TYPE = 2294272;
    //hex: 0x230201
    public static final int RESPONSE_MESSAGE_TYPE = 2294273;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private CPMapPutCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * CP group ID of this CPMap instance.
         */
        public com.hazelcast.cp.internal.RaftGroupId groupId;

        /**
         * Name of this CPMap instance.
         */
        public java.lang.String name;

        /**
         * Key of the value.
         */
        public com.hazelcast.internal.serialization.Data key;

        /**
         * Value to associate with the key.
         */
        public com.hazelcast.internal.serialization.Data value;
    }

    public static ClientMessage encodeRequest(com.hazelcast.cp.internal.RaftGroupId groupId, java.lang.String name, com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setContainsSerializedDataInRequest(true);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("CPMap.Put");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        clientMessage.add(initialFrame);
        RaftGroupIdCodec.encode(clientMessage, groupId);
        StringCodec.encode(clientMessage, name);
        DataCodec.encode(clientMessage, key);
        DataCodec.encode(clientMessage, value);
        return clientMessage;
    }

    public static CPMapPutCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        //empty initial frame
        iterator.next();
        request.groupId = RaftGroupIdCodec.decode(iterator);
        request.name = StringCodec.decode(iterator);
        request.key = DataCodec.decode(iterator);
        request.value = DataCodec.decode(iterator);
        return request;
    }

    public static ClientMessage encodeResponse(@Nullable com.hazelcast.internal.serialization.Data response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, response, DataCodec::encode);
        return clientMessage;
    }

    /**
     * Previous value associated with the key. 
     */
    public static com.hazelcast.internal.serialization.Data decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        //empty initial frame
        iterator.next();
        return CodecUtil.decodeNullable(iterator, DataCodec::decode);
    }
}

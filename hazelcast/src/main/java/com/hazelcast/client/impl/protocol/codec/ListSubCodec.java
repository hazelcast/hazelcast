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
 * Returns a view of the portion of this list between the specified from, inclusive, and to, exclusive.(If from and
 * to are equal, the returned list is empty.) The returned list is backed by this list, so non-structural changes in
 * the returned list are reflected in this list, and vice-versa. The returned list supports all of the optional list
 * operations supported by this list.
 * This method eliminates the need for explicit range operations (of the sort that commonly exist for arrays).
 * Any operation that expects a list can be used as a range operation by passing a subList view instead of a whole list.
 * Similar idioms may be constructed for indexOf and lastIndexOf, and all of the algorithms in the Collections class
 * can be applied to a subList.
 * The semantics of the list returned by this method become undefined if the backing list (i.e., this list) is
 * structurally modified in any way other than via the returned list.(Structural modifications are those that change
 * the size of this list, or otherwise perturb it in such a fashion that iterations in progress may yield incorrect results.)
 */
@Generated("68fb3cc99c6054e9d362bf98cd3b37c7")
public final class ListSubCodec {
    //hex: 0x051500
    public static final int REQUEST_MESSAGE_TYPE = 333056;
    //hex: 0x051501
    public static final int RESPONSE_MESSAGE_TYPE = 333057;
    private static final int REQUEST_FROM_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_TO_FIELD_OFFSET = REQUEST_FROM_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_TO_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private ListSubCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the List
         */
        public java.lang.String name;

        /**
         * Low endpoint (inclusive) of the subList
         */
        public int from;

        /**
         * High endpoint (exclusive) of the subList
         */
        public int to;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int from, int to) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("List.Sub");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, REQUEST_FROM_FIELD_OFFSET, from);
        encodeInt(initialFrame.content, REQUEST_TO_FIELD_OFFSET, to);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static ListSubCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.from = decodeInt(initialFrame.content, REQUEST_FROM_FIELD_OFFSET);
        request.to = decodeInt(initialFrame.content, REQUEST_TO_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
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
     * A view of the specified range within this list
     */
    public static java.util.List<com.hazelcast.internal.serialization.Data> decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        //empty initial frame
        iterator.next();
        return ListMultiFrameCodec.decode(iterator, DataCodec::decode);
    }
}

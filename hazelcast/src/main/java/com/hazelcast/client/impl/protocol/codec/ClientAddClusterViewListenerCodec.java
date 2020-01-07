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
import com.hazelcast.logging.Logger;

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
 * TODO DOC
 */
@Generated("aa82d8590192c2e40b614e82b67811db")
public final class ClientAddClusterViewListenerCodec {
    //hex: 0x000300
    public static final int REQUEST_MESSAGE_TYPE = 768;
    //hex: 0x000301
    public static final int RESPONSE_MESSAGE_TYPE = 769;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_MEMBERS_VIEW_VERSION_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_MEMBERS_VIEW_INITIAL_FRAME_SIZE = EVENT_MEMBERS_VIEW_VERSION_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    //hex: 0x000302
    private static final int EVENT_MEMBERS_VIEW_MESSAGE_TYPE = 770;
    private static final int EVENT_PARTITIONS_VIEW_VERSION_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_PARTITIONS_VIEW_INITIAL_FRAME_SIZE = EVENT_PARTITIONS_VIEW_VERSION_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    //hex: 0x000303
    private static final int EVENT_PARTITIONS_VIEW_MESSAGE_TYPE = 771;

    private ClientAddClusterViewListenerCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {
    }

    public static ClientMessage encodeRequest() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Client.AddClusterViewListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        clientMessage.add(initialFrame);
        return clientMessage;
    }

    public static ClientAddClusterViewListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        //empty initial frame
        iterator.next();
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static ClientAddClusterViewListenerCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

    public static ClientMessage encodeMembersViewEvent(int version, java.util.Collection<com.hazelcast.internal.cluster.MemberInfo> memberInfos) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_MEMBERS_VIEW_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_MEMBERS_VIEW_MESSAGE_TYPE);
        encodeInt(initialFrame.content, EVENT_MEMBERS_VIEW_VERSION_FIELD_OFFSET, version);
        clientMessage.add(initialFrame);

        ListMultiFrameCodec.encode(clientMessage, memberInfos, MemberInfoCodec::encode);
        return clientMessage;
    }
    public static ClientMessage encodePartitionsViewEvent(int version, java.util.Collection<java.util.Map.Entry<com.hazelcast.cluster.Address, java.util.List<java.lang.Integer>>> partitions) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_PARTITIONS_VIEW_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_PARTITIONS_VIEW_MESSAGE_TYPE);
        encodeInt(initialFrame.content, EVENT_PARTITIONS_VIEW_VERSION_FIELD_OFFSET, version);
        clientMessage.add(initialFrame);

        EntryListCodec.encode(clientMessage, partitions, AddressCodec::encode, ListIntegerCodec::encode);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
            if (messageType == EVENT_MEMBERS_VIEW_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                int version = decodeInt(initialFrame.content, EVENT_MEMBERS_VIEW_VERSION_FIELD_OFFSET);
                java.util.Collection<com.hazelcast.internal.cluster.MemberInfo> memberInfos = ListMultiFrameCodec.decode(iterator, MemberInfoCodec::decode);
                handleMembersViewEvent(version, memberInfos);
                return;
            }
            if (messageType == EVENT_PARTITIONS_VIEW_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                int version = decodeInt(initialFrame.content, EVENT_PARTITIONS_VIEW_VERSION_FIELD_OFFSET);
                java.util.Collection<java.util.Map.Entry<com.hazelcast.cluster.Address, java.util.List<java.lang.Integer>>> partitions = EntryListCodec.decode(iterator, AddressCodec::decode, ListIntegerCodec::decode);
                handlePartitionsViewEvent(version, partitions);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }
        public abstract void handleMembersViewEvent(int version, java.util.Collection<com.hazelcast.internal.cluster.MemberInfo> memberInfos);
        public abstract void handlePartitionsViewEvent(int version, java.util.Collection<java.util.Map.Entry<com.hazelcast.cluster.Address, java.util.List<java.lang.Integer>>> partitions);
    }
}

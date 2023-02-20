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
 * Adds a cluster view listener to a connection.
 */
@Generated("1cd775b33c778bc96ee05b73edf7277b")
public final class ClientAddClusterViewListenerCodec {
    //hex: 0x000300
    public static final int REQUEST_MESSAGE_TYPE = 768;
    //hex: 0x000301
    public static final int RESPONSE_MESSAGE_TYPE = 769;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
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

    public static ClientMessage encodeRequest() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Client.AddClusterViewListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        clientMessage.add(initialFrame);
        return clientMessage;
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static ClientMessage encodeMembersViewEvent(int version, java.util.Collection<com.hazelcast.internal.cluster.MemberInfo> memberInfos) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_MEMBERS_VIEW_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_MEMBERS_VIEW_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, EVENT_MEMBERS_VIEW_VERSION_FIELD_OFFSET, version);
        clientMessage.add(initialFrame);

        ListMultiFrameCodec.encode(clientMessage, memberInfos, MemberInfoCodec::encode);
        return clientMessage;
    }

    public static ClientMessage encodePartitionsViewEvent(int version, java.util.Collection<java.util.Map.Entry<java.util.UUID, java.util.List<java.lang.Integer>>> partitions) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_PARTITIONS_VIEW_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_PARTITIONS_VIEW_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, EVENT_PARTITIONS_VIEW_VERSION_FIELD_OFFSET, version);
        clientMessage.add(initialFrame);

        EntryListUUIDListIntegerCodec.encode(clientMessage, partitions);
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
                java.util.Collection<java.util.Map.Entry<java.util.UUID, java.util.List<java.lang.Integer>>> partitions = EntryListUUIDListIntegerCodec.decode(iterator);
                handlePartitionsViewEvent(version, partitions);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }

        /**
         * @param version Incremental member list version
         * @param memberInfos List of member infos  at the cluster associated with the given version
         *                    params:
         */
        public abstract void handleMembersViewEvent(int version, java.util.Collection<com.hazelcast.internal.cluster.MemberInfo> memberInfos);

        /**
         * @param version Incremental state version of the partition table
         * @param partitions The partition table. In each entry, it has uuid of the member and list of partitions belonging to that member
         */
        public abstract void handlePartitionsViewEvent(int version, java.util.Collection<java.util.Map.Entry<java.util.UUID, java.util.List<java.lang.Integer>>> partitions);
    }
}

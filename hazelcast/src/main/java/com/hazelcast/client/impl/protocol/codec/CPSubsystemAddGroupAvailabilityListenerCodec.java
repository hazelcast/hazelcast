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
 * Registers a new CP group availability listener.
 */
@Generated("50f41213c3feee3460232d08ec934646")
public final class CPSubsystemAddGroupAvailabilityListenerCodec {
    //hex: 0x220300
    public static final int REQUEST_MESSAGE_TYPE = 2228992;
    //hex: 0x220301
    public static final int RESPONSE_MESSAGE_TYPE = 2228993;
    private static final int REQUEST_LOCAL_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_LOCAL_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_GROUP_AVAILABILITY_EVENT_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    //hex: 0x220302
    private static final int EVENT_GROUP_AVAILABILITY_EVENT_MESSAGE_TYPE = 2228994;

    private CPSubsystemAddGroupAvailabilityListenerCodec() {
    }

    public static ClientMessage encodeRequest(boolean local) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("CPSubsystem.AddGroupAvailabilityListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeBoolean(initialFrame.content, REQUEST_LOCAL_FIELD_OFFSET, local);
        clientMessage.add(initialFrame);
        return clientMessage;
    }

    /**
     * Denotes whether register a local listener or not.
     */
    public static boolean decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeBoolean(initialFrame.content, REQUEST_LOCAL_FIELD_OFFSET);
    }

    public static ClientMessage encodeResponse(java.util.UUID response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeUUID(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    /**
     * Registration id for the listener.
     */
    public static java.util.UUID decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeUUID(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }

    public static ClientMessage encodeGroupAvailabilityEventEvent(com.hazelcast.cp.internal.RaftGroupId groupId, java.util.Collection<com.hazelcast.cp.CPMember> members, java.util.Collection<com.hazelcast.cp.CPMember> unavailableMembers) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_GROUP_AVAILABILITY_EVENT_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_GROUP_AVAILABILITY_EVENT_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        clientMessage.add(initialFrame);

        RaftGroupIdCodec.encode(clientMessage, groupId);
        ListMultiFrameCodec.encode(clientMessage, members, CPMemberCodec::encode);
        ListMultiFrameCodec.encode(clientMessage, unavailableMembers, CPMemberCodec::encode);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
            if (messageType == EVENT_GROUP_AVAILABILITY_EVENT_MESSAGE_TYPE) {
                //empty initial frame
                iterator.next();
                com.hazelcast.cp.internal.RaftGroupId groupId = RaftGroupIdCodec.decode(iterator);
                java.util.Collection<com.hazelcast.cp.CPMember> members = ListMultiFrameCodec.decode(iterator, CPMemberCodec::decode);
                java.util.Collection<com.hazelcast.cp.CPMember> unavailableMembers = ListMultiFrameCodec.decode(iterator, CPMemberCodec::decode);
                handleGroupAvailabilityEventEvent(groupId, members, unavailableMembers);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }

        /**
         * @param groupId Group id whose availability is reported.
         * @param members All members.
         * @param unavailableMembers Missing members.
         */
        public abstract void handleGroupAvailabilityEventEvent(com.hazelcast.cp.internal.RaftGroupId groupId, java.util.Collection<com.hazelcast.cp.CPMember> members, java.util.Collection<com.hazelcast.cp.CPMember> unavailableMembers);
    }
}

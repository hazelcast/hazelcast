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
 * Subscribes to this topic. When someone publishes a message on this topic. onMessage() function of the given
 * MessageListener is called. More than one message listener can be added on one instance.
 */
@Generated("8a95bf1f1999015e9036caa2f74fdd0c")
public final class TopicAddMessageListenerCodec {
    //hex: 0x040200
    public static final int REQUEST_MESSAGE_TYPE = 262656;
    //hex: 0x040201
    public static final int RESPONSE_MESSAGE_TYPE = 262657;
    private static final int REQUEST_LOCAL_ONLY_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_LOCAL_ONLY_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_TOPIC_PUBLISH_TIME_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_TOPIC_UUID_FIELD_OFFSET = EVENT_TOPIC_PUBLISH_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int EVENT_TOPIC_INITIAL_FRAME_SIZE = EVENT_TOPIC_UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    //hex: 0x040202
    private static final int EVENT_TOPIC_MESSAGE_TYPE = 262658;

    private TopicAddMessageListenerCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the Topic
         */
        public java.lang.String name;

        /**
         * if true listens only local events on registered member
         */
        public boolean localOnly;
    }

    public static ClientMessage encodeRequest(java.lang.String name, boolean localOnly) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Topic.AddMessageListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET, localOnly);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static TopicAddMessageListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.localOnly = decodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        return request;
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
     * returns the registration id
     */
    public static java.util.UUID decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeUUID(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }

    public static ClientMessage encodeTopicEvent(com.hazelcast.internal.serialization.Data item, long publishTime, java.util.UUID uuid) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_TOPIC_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_TOPIC_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, EVENT_TOPIC_PUBLISH_TIME_FIELD_OFFSET, publishTime);
        encodeUUID(initialFrame.content, EVENT_TOPIC_UUID_FIELD_OFFSET, uuid);
        clientMessage.add(initialFrame);

        DataCodec.encode(clientMessage, item);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
            if (messageType == EVENT_TOPIC_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                long publishTime = decodeLong(initialFrame.content, EVENT_TOPIC_PUBLISH_TIME_FIELD_OFFSET);
                java.util.UUID uuid = decodeUUID(initialFrame.content, EVENT_TOPIC_UUID_FIELD_OFFSET);
                com.hazelcast.internal.serialization.Data item = DataCodec.decode(iterator);
                handleTopicEvent(item, publishTime, uuid);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }

        /**
         * @param item Item that the event is fired for.
         * @param publishTime Time that the item is published to the topic.
         * @param uuid UUID of the member that dispatches this event.
         */
        public abstract void handleTopicEvent(com.hazelcast.internal.serialization.Data item, long publishTime, java.util.UUID uuid);
    }
}

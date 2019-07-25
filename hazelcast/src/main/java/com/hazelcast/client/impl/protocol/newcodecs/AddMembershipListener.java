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

package com.hazelcast.client.impl.protocol.newcodecs;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cluster.Member;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Bits;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.CORRELATION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.DEFAULT_FLAGS;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.NULL_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.PARTITION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.TYPE_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;

public class AddMembershipListener {

    public static class Request {
        private static final int LOCAL_ONLY = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;
        private static final int HEADER_SIZE = LOCAL_ONLY + Bits.BOOLEAN_SIZE_IN_BYTES;

        public static final int TYPE = 0;//ClientMessageType.CLIENT_ADDMEMBERSHIPLISTENER.id();

        public boolean localOnly;

        public static ClientMessage encode(boolean localOnly) {
            ClientMessage clientMessage = ClientMessage.createForEncode();
            clientMessage.setRetryable(false);
            clientMessage.setAcquiresResource(false);
            clientMessage.setOperationName("Client.addPartitionListener");

            ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGMENTED_MESSAGE);
            Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);
            initialFrame.content[LOCAL_ONLY] = (byte) (localOnly ? 1 : 0);

            clientMessage.addFrame(initialFrame);

            return clientMessage;
        }

        public static Request decode(ClientMessage clientMessage) {
            Iterator<ClientMessage.Frame> iterator = clientMessage.iterator();
            Request request = new Request();

            ClientMessage.Frame initialFrame = iterator.next();
            request.localOnly = initialFrame.content[LOCAL_ONLY] == 1;
            return request;
        }
    }

    public static class Response {
        private static final int HEADER_SIZE = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;
        public static final int TYPE = 104;

        public String uuid;

        public static ClientMessage encode(String uuid) {
            ClientMessage clientMessage = ClientMessage.createForEncode();
            ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGMENTED_MESSAGE);

            Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);
            clientMessage.addFrame(initialFrame);

            clientMessage.addFrame(new ClientMessage.Frame(uuid.getBytes(Bits.UTF_8), DEFAULT_FLAGS));

            return clientMessage;
        }

        public static Response decode(ClientMessage clientMessage) {
            Iterator<ClientMessage.Frame> iterator = clientMessage.iterator();

            Response response = new Response();

            ClientMessage.Frame frame = iterator.next();

            response.uuid = new String(iterator.next().content, Bits.UTF_8);
            return response;
        }
    }

    public static class Event {

        public static class MemberEvent {
            public static final int TYPE = 200;
            public static final int EVENT_TYPE = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;
            public static final int HEADER_SIZE = EVENT_TYPE + Bits.INT_SIZE_IN_BYTES;

            public static ClientMessage encode(Member member, int eventType) {

                ClientMessage clientMessage = ClientMessage.createForEncode();
                ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGMENTED_MESSAGE);
                initialFrame.flags |= ClientMessage.IS_EVENT;

                Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);
                Bits.writeIntL(initialFrame.content, EVENT_TYPE, eventType);

                clientMessage.addFrame(initialFrame);
                clientMessage.addFrame(BEGIN_FRAME);
                MemberCodec.encode(clientMessage, member);
                clientMessage.addFrame(END_FRAME);

                return clientMessage;
            }
        }

        public static class MemberListEvent {
            public static final int TYPE = 201;
            public static final int HEADER_SIZE = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;

            public static ClientMessage encode(Collection<Member> members) {

                ClientMessage clientMessage = ClientMessage.createForEncode();
                ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGMENTED_MESSAGE);
                initialFrame.flags |= ClientMessage.IS_EVENT;

                Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);

                clientMessage.addFrame(initialFrame);
                clientMessage.addFrame(BEGIN_FRAME);
                for (Member member : members) {
                    clientMessage.addFrame(BEGIN_FRAME);
                    MemberCodec.encode(clientMessage, member);
                    clientMessage.addFrame(END_FRAME);
                }
                clientMessage.addFrame(END_FRAME);

                return clientMessage;
            }
        }

        public static class MemberAttributeChangeEvent {
            public static final int TYPE = 202;
            public static final int OPERATION_TYPE = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;
            public static final int HEADER_SIZE = OPERATION_TYPE + Bits.INT_SIZE_IN_BYTES;

            public static ClientMessage encode(String uuid, String key, int operationType, String value) {
                ClientMessage clientMessage = ClientMessage.createForEncode();
                ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGMENTED_MESSAGE);
                initialFrame.flags |= ClientMessage.IS_EVENT;
                Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);
                Bits.writeIntL(initialFrame.content, OPERATION_TYPE, operationType);

                clientMessage.addFrame(initialFrame);
                clientMessage.addFrame(new ClientMessage.Frame(uuid.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
                clientMessage.addFrame(new ClientMessage.Frame(key.getBytes(Bits.UTF_8), DEFAULT_FLAGS));

                if (value == null) {
                    clientMessage.addFrame(NULL_FRAME);
                } else {
                    clientMessage.addFrame(new ClientMessage.Frame(value.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
                }
                return clientMessage;
            }
        }


        public abstract static class AbstractEventHandler {
            public AbstractEventHandler() {
            }

            public void handle(ClientMessage clientMessage) {
                int messageType = clientMessage.getMessageType();

                Iterator<ClientMessage.Frame> iterator = clientMessage.iterator();
                ClientMessage.Frame frame;
                switch (messageType) {
                    case MemberEvent.TYPE:
                        frame = iterator.next();
                        int eventType = Bits.readIntL(frame.content, MemberEvent.EVENT_TYPE);

                        iterator.next();//begin
                        Member member = MemberCodec.decode(iterator);
//                        for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {
//
//                        }
                        handleMemberEventV10(member, eventType);
                        break;
                    case MemberListEvent.TYPE:
                        frame = iterator.next();

                        Collection<Member> members = new LinkedList<>();
                        iterator.next();//list begin
                        for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {
                            members.add(MemberCodec.decode(iterator));
                            for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {

                            }
                        }
                        handleMemberListEventV10(members);
                        break;
                    case MemberAttributeChangeEvent.TYPE:
                        frame = iterator.next();
                        int operationType = Bits.readIntL(frame.content, MemberAttributeChangeEvent.OPERATION_TYPE);

                        String uuid = new String(iterator.next().content, Bits.UTF_8);
                        String key = new String(iterator.next().content, Bits.UTF_8);

                        String value = null;
                        frame = iterator.next();
                        if (!frame.isNullFrame()) {
                            value = new String(frame.content, Bits.UTF_8);
                        }
                        handleMemberAttributeChangeEventV10(uuid, key, operationType, value);
                        break;
                    default:
                        Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
                }

            }

            public abstract void handleMemberEventV10(Member member, int eventType);

            public abstract void handleMemberListEventV10(Collection<Member> members);

            public abstract void handleMemberAttributeChangeEventV10(String uuid, String key, int operationType, String value);

        }
    }

}

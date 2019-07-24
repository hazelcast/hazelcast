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
//import com.hazelcast.client.impl.protocol.codec.ClientMessageType;
import com.hazelcast.cluster.Member;
import com.hazelcast.nio.Bits;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Optional;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.CORRELATION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.DEFAULT_FLAGS;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.NULL_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.TYPE_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;

public class Authentication {

    public static class Request {
        // fixed request fields
        private static final int IS_OWNER_CONNECTION = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;
        private static final int SERIALIZATION_VERSION = IS_OWNER_CONNECTION + Bits.BOOLEAN_SIZE_IN_BYTES;
        private static final int PARTITION_COUNT = SERIALIZATION_VERSION + Bits.BYTE_SIZE_IN_BYTES;
        private static final int HEADER_SIZE = PARTITION_COUNT + Bits.INT_SIZE_IN_BYTES;

        public static final int TYPE = 0;//ClientMessageType.CLIENT_AUTHENTICATION.id();

        /**
         * @since 1.0
         */
        public String username;
        public String password;
        public String uuid;
        public String ownerUuid;
        public boolean isOwnerConnection;
        public String clientType;
        public byte serializationVersion;
        /**
         * @since 1.3
         */
        public Optional<String> clientHazelcastVersion = Optional.empty();
        /**
         * @since 1.8
         */
        public Optional<String> clientName = Optional.empty();
        /**
         * @since 1.8
         */
        public Optional<java.util.Collection<String>> labels = Optional.empty();
        /**
         * @since 1.8
         */
        public Optional<Integer> partitionCount = Optional.empty();
        /**
         * @since 1.8
         */
        public Optional<String> clusterId = Optional.empty();

        public static ClientMessage encode(String username, String password, String uuid, String ownerUuid,
                                           boolean isOwnerConnection, String clientType, byte serializationVersion,
                                           String clientHazelcastVersion, String clientName,
                                           java.util.Collection<String> labels, Integer partitionCount, String clusterId) {
            ClientMessage clientMessage = ClientMessage.createForEncode();
            clientMessage.setRetryable(false);
            clientMessage.setAcquiresResource(false);
            clientMessage.setOperationName("Client.authentication");

            ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGMENTED_MESSAGE);
            Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);
            initialFrame.content[IS_OWNER_CONNECTION] = isOwnerConnection ? (byte) 1 : (byte) 0;
            initialFrame.content[SERIALIZATION_VERSION] = serializationVersion;
            Bits.writeIntL(initialFrame.content, PARTITION_COUNT, partitionCount == null ? -1 : partitionCount);

            clientMessage.addFrame(initialFrame);
            clientMessage.addFrame(new ClientMessage.Frame(username.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            clientMessage.addFrame(new ClientMessage.Frame(password.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            if (uuid == null) {
                clientMessage.addFrame(NULL_FRAME);
            } else {
                clientMessage.addFrame(new ClientMessage.Frame(uuid.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            }
            if (ownerUuid == null) {
                clientMessage.addFrame(NULL_FRAME);
            } else {
                clientMessage.addFrame(new ClientMessage.Frame(ownerUuid.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            }
            clientMessage.addFrame(new ClientMessage.Frame(clientType.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            clientMessage.addFrame(new ClientMessage.Frame(clientHazelcastVersion.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            clientMessage.addFrame(new ClientMessage.Frame(clientName.getBytes(Bits.UTF_8), DEFAULT_FLAGS));


            clientMessage.addFrame(BEGIN_FRAME);
            for (String label : labels) {
                clientMessage.addFrame(new ClientMessage.Frame(label.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            }
            clientMessage.addFrame(END_FRAME);

            if (clusterId == null) {
                clientMessage.addFrame(NULL_FRAME);
            } else {
                clientMessage.addFrame(new ClientMessage.Frame(clusterId.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            }

            return clientMessage;
        }

        public static Request decode(ClientMessage clientMessage) {
            Iterator<ClientMessage.Frame> iterator = clientMessage.iterator();
            ClientMessage.Frame initialFrame = iterator.next();

            Request request = new Request();
            request.isOwnerConnection = initialFrame.content[IS_OWNER_CONNECTION] == (byte) 1;
            request.serializationVersion = initialFrame.content[SERIALIZATION_VERSION];

            //since 1.3
            //TODO Sancar Nullable in primitives ???
            if (initialFrame.content.length != PARTITION_COUNT) {
                int value = Bits.readIntL(initialFrame.content, PARTITION_COUNT);
                request.partitionCount = value == -1 ? Optional.empty() : Optional.of(value);
            }

            request.username = new String(iterator.next().content, Bits.UTF_8);
            request.password = new String(iterator.next().content, Bits.UTF_8);
            ClientMessage.Frame frame = iterator.next();
            if (!frame.isNullFrame()) {
                request.uuid = new String(frame.content, Bits.UTF_8);
            }
            frame = iterator.next();
            if (!frame.isNullFrame()) {
                request.ownerUuid = new String(frame.content, Bits.UTF_8);
            }
            request.clientType = new String(iterator.next().content, Bits.UTF_8);

            //since 1.3
            if (!iterator.hasNext()) {
                return request;
            }

            request.clientHazelcastVersion = Optional.of(new String(iterator.next().content, Bits.UTF_8));

            //since 1.8
            if (!iterator.hasNext()) {
                return request;
            }

            request.clientName = Optional.of(new String(iterator.next().content, Bits.UTF_8));

            LinkedList<String> labels = new LinkedList<>();
            iterator.next();//BEGIN
            for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {
                labels.add(new String(frame.content, Bits.UTF_8));
            }
            request.labels = Optional.of(labels);

            frame = iterator.next();
            if (!frame.isNullFrame()) {
                request.clusterId = Optional.of(new String(frame.content, Bits.UTF_8));
            }

            return request;
        }

    }

    public static class Response {
        //fixed response fields
        private static final int STATUS = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;
        private static final int SERIALIZATION_VERSION = STATUS + Bits.LONG_SIZE_IN_BYTES;
        private static final int PARTITION_COUNT = SERIALIZATION_VERSION + Bits.BYTE_SIZE_IN_BYTES;
        private static final int HEADER_SIZE = PARTITION_COUNT + Bits.INT_SIZE_IN_BYTES;

        private static final int TYPE = 107;


        /**
         * @since 1.0
         */
        public byte status;
        public com.hazelcast.nio.Address address;
        public String uuid;
        public String ownerUuid;
        public byte serializationVersion;
        /**
         * @since 1.3
         */
        public Optional<String> serverHazelcastVersion = Optional.empty();
        /**
         * @since 1.3
         */
        public Optional<java.util.List<Member>> clientUnregisteredMembers = Optional.empty();
        /**
         * @since 1.8
         */
        public Optional<Integer> partitionCount = Optional.empty();
        /**
         * @since 1.8
         */
        public Optional<String> clusterId = Optional.empty();


        public static ClientMessage encode(byte status, com.hazelcast.nio.Address address, String uuid, String ownerUuid,
                                           byte serializationVersion, String serverHazelcastVersion,
                                           Collection<Member> clientUnregisteredMembers, int partitionCount, String clusterId) {
            ClientMessage clientMessage = ClientMessage.createForEncode();

            ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGMENTED_MESSAGE);
            Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);
            initialFrame.content[STATUS] = status;
            initialFrame.content[SERIALIZATION_VERSION] = serializationVersion;
            Bits.writeIntL(initialFrame.content, PARTITION_COUNT, partitionCount);

            clientMessage.addFrame(initialFrame);

            clientMessage.addFrame(BEGIN_FRAME);
            AddressCodec.encode(clientMessage, address);
            clientMessage.addFrame(END_FRAME);

            if (uuid == null) {
                clientMessage.addFrame(NULL_FRAME);
            } else {
                clientMessage.addFrame(new ClientMessage.Frame(uuid.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            }
            if (ownerUuid == null) {
                clientMessage.addFrame(NULL_FRAME);
            } else {
                clientMessage.addFrame(new ClientMessage.Frame(ownerUuid.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            }
            clientMessage.addFrame(new ClientMessage.Frame(serverHazelcastVersion.getBytes(Bits.UTF_8), DEFAULT_FLAGS));

            clientMessage.addFrame(BEGIN_FRAME);
            for (Member member : clientUnregisteredMembers) {
                clientMessage.addFrame(BEGIN_FRAME);
                MemberCodec.encode(clientMessage, member);
                clientMessage.addFrame(END_FRAME);
            }
            clientMessage.addFrame(END_FRAME);

            clientMessage.addFrame(new ClientMessage.Frame(clusterId.getBytes(Bits.UTF_8), DEFAULT_FLAGS));
            return clientMessage;
        }

        public static Response decode(ClientMessage clientMessage) {
            Iterator<ClientMessage.Frame> iterator = clientMessage.iterator();
            Response response = new Response();

            ClientMessage.Frame initialFrame = iterator.next();

            response.status = initialFrame.content[STATUS];
            response.serializationVersion = initialFrame.content[SERIALIZATION_VERSION];
            //since 1.3
            if (initialFrame.content.length != PARTITION_COUNT) {
                response.partitionCount = Optional.of(Bits.readIntL(initialFrame.content, PARTITION_COUNT));
            }

            ClientMessage.Frame frame = iterator.next();//begin
            response.address = AddressCodec.decode(iterator);
            for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {

            }

            frame = iterator.next();
            if (!frame.isNullFrame()) {
                response.uuid = new String(frame.content, Bits.UTF_8);
            }
            frame = iterator.next();
            if (!frame.isNullFrame()) {
                response.ownerUuid = new String(frame.content, Bits.UTF_8);
            }

            if (!iterator.hasNext()) {
                return response;
            }

            response.serverHazelcastVersion = Optional.of(new String(iterator.next().content, Bits.UTF_8));

            response.clientUnregisteredMembers = Optional.of(new LinkedList<>());

            iterator.next();//list begin
            for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {
                response.clientUnregisteredMembers.get().add(MemberCodec.decode(iterator));
                for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {

                }
            }

            response.clusterId = Optional.of(new String(iterator.next().content, Bits.UTF_8));
            return response;
        }
    }
}

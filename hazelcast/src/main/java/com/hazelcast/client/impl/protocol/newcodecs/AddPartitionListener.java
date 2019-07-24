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
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.CORRELATION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.DEFAULT_FLAGS;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.PARTITION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.TYPE_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;

public class AddPartitionListener {


    public static class Request {
        private static final int HEADER_SIZE = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;

        public static final int TYPE = 0;//ClientMessageType.CLIENT_ADDPARTITIONLISTENER.id();

        public static ClientMessage encode() {
            ClientMessage clientMessage = ClientMessage.createForEncode();
            clientMessage.setRetryable(false);
            clientMessage.setAcquiresResource(false);
            clientMessage.setOperationName("Client.addPartitionListener");

            ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGMENTED_MESSAGE);
            Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);

            clientMessage.addFrame(initialFrame);

            return clientMessage;
        }

        public static Request decode(ClientMessage clientMessage) {
            Iterator<ClientMessage.Frame> iterator = clientMessage.iterator();
            Request request = new Request();
            return request;
        }
    }

    public static class Response {
        private static final int HEADER_SIZE = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;

        public static final int TYPE = 100;

        public static ClientMessage encode() {
            ClientMessage clientMessage = ClientMessage.createForEncode();
            ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGMENTED_MESSAGE);
            clientMessage.addFrame(initialFrame);

            Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);

            return clientMessage;
        }

        public static Response decode(ClientMessage clientMessage) {
            Iterator<ClientMessage.Frame> iterator = clientMessage.iterator();

            Response response = new Response();

            return response;
        }
    }

    public static class Event {

        public static class PartitionsEvent {
            private static final int PARTITION_STATE_VERSION = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;
            private static final int HEADER_SIZE = PARTITION_STATE_VERSION + Bits.INT_SIZE_IN_BYTES;
            public static final int TYPE = 217;

            public static ClientMessage encodePartitionsEvent(Map<Address, List<Integer>> partitions, int partitionStateVersion) {
                ClientMessage clientMessage = ClientMessage.createForEncode();
                ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[HEADER_SIZE], UNFRAGMENTED_MESSAGE);
                initialFrame.flags |= ClientMessage.IS_EVENT;
                Bits.writeIntL(initialFrame.content, TYPE_FIELD_OFFSET, TYPE);
                Bits.writeIntL(initialFrame.content, PARTITION_STATE_VERSION, partitionStateVersion);

                clientMessage.addFrame(initialFrame);

                clientMessage.addFrame(BEGIN_FRAME);
                for (Address address : partitions.keySet()) {
                    clientMessage.addFrame(BEGIN_FRAME);
                    AddressCodec.encode(clientMessage, address);
                    clientMessage.addFrame(END_FRAME);
                }
                clientMessage.addFrame(END_FRAME);

                clientMessage.addFrame(BEGIN_FRAME);
                for (List<Integer> value : partitions.values()) {
                    int length = value.size() * Bits.INT_SIZE_IN_BYTES;
                    ClientMessage.Frame fixedSizedParamListFrame = new ClientMessage.Frame(new byte[length], DEFAULT_FLAGS);
                    int index = 0;
                    for (Integer integer : value) {
                        Bits.writeIntL(fixedSizedParamListFrame.content, index, integer);
                        index += Bits.INT_SIZE_IN_BYTES;
                    }
                    clientMessage.addFrame(fixedSizedParamListFrame);
                }
                clientMessage.addFrame(END_FRAME);

                return clientMessage;
            }
        }

        public abstract static class AbstractEventHandler {

            public void handle(ClientMessage clientMessage) {
                int messageType = clientMessage.getMessageType();
                switch (messageType) {
                    case PartitionsEvent.TYPE:
                        Iterator<ClientMessage.Frame> iterator = clientMessage.iterator();
                        ClientMessage.Frame frame = iterator.next();

                        int partitionStateVersion = Bits.readIntL(frame.content, PartitionsEvent.PARTITION_STATE_VERSION);

                        Map<Address, List<Integer>> partitionsMap = new HashMap<>();
                        List<Address> keys = new LinkedList<>();

                        iterator.next();//list begin
                        for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {
                            keys.add(AddressCodec.decode(iterator));
                            for (frame = iterator.next(); !frame.isDataStructureEndFrame(); frame = iterator.next()) {

                            }
                        }

                        iterator.next();//list begin
                        for (Address key : keys) {
                            ClientMessage.Frame fixedSizedParamListFrame = iterator.next();
                            List<Integer> partitions = new LinkedList<>();
                            for (int i = 0; i < fixedSizedParamListFrame.content.length; i += Bits.INT_SIZE_IN_BYTES) {
                                partitions.add(Bits.readIntL(fixedSizedParamListFrame.content, i));
                            }
                            partitionsMap.put(key, partitions);
                        }
                        iterator.next();//list end
                        handlePartitionsEventV15(partitionsMap, partitionStateVersion);
                        return;
                    default:
                        Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
                }


            }

            public abstract void handlePartitionsEventV15(Map<Address, List<Integer>> partitions, int partitionStateVersion);
        }
    }
}

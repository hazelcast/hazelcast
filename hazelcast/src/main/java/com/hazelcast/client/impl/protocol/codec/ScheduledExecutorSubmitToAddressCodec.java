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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/**
 * Submits the task to a member for execution, member is provided in the form of an address.
 */
public class ScheduledExecutorSubmitToAddressCodec {

        private static final int REQUEST_TYPE_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
        private static final int REQUEST_INITIAL_DELAY_IN_MILLIS_FIELD_OFFSET = REQUEST_TYPE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        private static final int REQUEST_PERIOD_IN_MILLIS_FIELD_OFFSET = REQUEST_INITIAL_DELAY_IN_MILLIS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_PERIOD_IN_MILLIS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int REQUEST_MESSAGE_TYPE = 7427;//hex: 0x1D03,
        private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int RESPONSE_MESSAGE_TYPE = 100;//hex: 0x0064,

    public static class RequestParameters {

        /**
         * The name of the scheduler.
         */
        public java.lang.String schedulerName;

        /**
         * The address of the member where the task will get scheduled.
         */
        public com.hazelcast.nio.Address address;

        /**
         * type of schedule logic, values 0 for SINGLE_RUN, 1 for AT_FIXED_RATE
         */
        public byte type;

        /**
         * The name of the task
         */
        public java.lang.String taskName;

        /**
         * Name The name of the task
         */
        public com.hazelcast.nio.serialization.Data task;

        /**
         * initial delay in milliseconds
         */
        public long initialDelayInMillis;

        /**
         * period between each run in milliseconds
         */
        public long periodInMillis;
    }

    public static ClientMessage encodeRequest(java.lang.String schedulerName, com.hazelcast.nio.Address address, byte type, java.lang.String taskName, com.hazelcast.nio.serialization.Data task, long initialDelayInMillis, long periodInMillis) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("ScheduledExecutor.SubmitToAddress");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeByte(initialFrame.content, REQUEST_TYPE_FIELD_OFFSET, type);
        encodeLong(initialFrame.content, REQUEST_INITIAL_DELAY_IN_MILLIS_FIELD_OFFSET, initialDelayInMillis);
        encodeLong(initialFrame.content, REQUEST_PERIOD_IN_MILLIS_FIELD_OFFSET, periodInMillis);
        clientMessage.addFrame(initialFrame);
        StringCodec.encode(clientMessage, schedulerName);
        AddressCodec.encode(clientMessage, address);
        StringCodec.encode(clientMessage, taskName);
        DataCodec.encode(clientMessage, task);
        return clientMessage;
    }

    public static ScheduledExecutorSubmitToAddressCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.type = decodeByte(initialFrame.content, REQUEST_TYPE_FIELD_OFFSET);
        request.initialDelayInMillis = decodeLong(initialFrame.content, REQUEST_INITIAL_DELAY_IN_MILLIS_FIELD_OFFSET);
        request.periodInMillis = decodeLong(initialFrame.content, REQUEST_PERIOD_IN_MILLIS_FIELD_OFFSET);
        request.schedulerName = StringCodec.decode(iterator);
        request.address = AddressCodec.decode(iterator);
        request.taskName = StringCodec.decode(iterator);
        request.task = DataCodec.decode(iterator);
        return request;
    }

    public static class ResponseParameters {
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.addFrame(initialFrame);

        return clientMessage;
    }

    public static ScheduledExecutorSubmitToAddressCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        ResponseParameters response = new ResponseParameters();
        iterator.next();//empty initial frame
        return response;
    }

}
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
 * Returns statistics of the task
 */
public final class ScheduledExecutorGetStatsFromPartitionCodec {
    //hex: 0x1D0500
    public static final int REQUEST_MESSAGE_TYPE = 1901824;
    //hex: 0x1D0501
    public static final int RESPONSE_MESSAGE_TYPE = 1901825;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_LAST_IDLE_TIME_NANOS_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_TOTAL_IDLE_TIME_NANOS_FIELD_OFFSET = RESPONSE_LAST_IDLE_TIME_NANOS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_TOTAL_RUNS_FIELD_OFFSET = RESPONSE_TOTAL_IDLE_TIME_NANOS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_TOTAL_RUN_TIME_NANOS_FIELD_OFFSET = RESPONSE_TOTAL_RUNS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_LAST_RUN_DURATION_NANOS_FIELD_OFFSET = RESPONSE_TOTAL_RUN_TIME_NANOS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_LAST_RUN_DURATION_NANOS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private ScheduledExecutorGetStatsFromPartitionCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * The name of the scheduler.
         */
        public java.lang.String schedulerName;

        /**
         * The name of the task
         */
        public java.lang.String taskName;
    }

    public static ClientMessage encodeRequest(java.lang.String schedulerName, java.lang.String taskName) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("ScheduledExecutor.GetStatsFromPartition");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, schedulerName);
        StringCodec.encode(clientMessage, taskName);
        return clientMessage;
    }

    public static ScheduledExecutorGetStatsFromPartitionCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        //empty initial frame
        iterator.next();
        request.schedulerName = StringCodec.decode(iterator);
        request.taskName = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * TODO DOC
         */
        public long lastIdleTimeNanos;

        /**
         * TODO DOC
         */
        public long totalIdleTimeNanos;

        /**
         * TODO DOC
         */
        public long totalRuns;

        /**
         * TODO DOC
         */
        public long totalRunTimeNanos;

        /**
         * TODO DOC
         */
        public long lastRunDurationNanos;
    }

    public static ClientMessage encodeResponse(long lastIdleTimeNanos, long totalIdleTimeNanos, long totalRuns, long totalRunTimeNanos, long lastRunDurationNanos) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        encodeLong(initialFrame.content, RESPONSE_LAST_IDLE_TIME_NANOS_FIELD_OFFSET, lastIdleTimeNanos);
        encodeLong(initialFrame.content, RESPONSE_TOTAL_IDLE_TIME_NANOS_FIELD_OFFSET, totalIdleTimeNanos);
        encodeLong(initialFrame.content, RESPONSE_TOTAL_RUNS_FIELD_OFFSET, totalRuns);
        encodeLong(initialFrame.content, RESPONSE_TOTAL_RUN_TIME_NANOS_FIELD_OFFSET, totalRunTimeNanos);
        encodeLong(initialFrame.content, RESPONSE_LAST_RUN_DURATION_NANOS_FIELD_OFFSET, lastRunDurationNanos);
        return clientMessage;
    }

    public static ScheduledExecutorGetStatsFromPartitionCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.lastIdleTimeNanos = decodeLong(initialFrame.content, RESPONSE_LAST_IDLE_TIME_NANOS_FIELD_OFFSET);
        response.totalIdleTimeNanos = decodeLong(initialFrame.content, RESPONSE_TOTAL_IDLE_TIME_NANOS_FIELD_OFFSET);
        response.totalRuns = decodeLong(initialFrame.content, RESPONSE_TOTAL_RUNS_FIELD_OFFSET);
        response.totalRunTimeNanos = decodeLong(initialFrame.content, RESPONSE_TOTAL_RUN_TIME_NANOS_FIELD_OFFSET);
        response.lastRunDurationNanos = decodeLong(initialFrame.content, RESPONSE_LAST_RUN_DURATION_NANOS_FIELD_OFFSET);
        return response;
    }

}

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
 * Adds a JobStatusListener to the specified job.
 */
@Generated("bf0d8bab26d197127125a7965998305d")
public final class JetAddJobStatusListenerCodec {
    //hex: 0xFE1300
    public static final int REQUEST_MESSAGE_TYPE = 16651008;
    //hex: 0xFE1301
    public static final int RESPONSE_MESSAGE_TYPE = 16651009;
    private static final int REQUEST_JOB_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_LOCAL_ONLY_FIELD_OFFSET = REQUEST_JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_LOCAL_ONLY_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int EVENT_JOB_STATUS_JOB_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_JOB_STATUS_PREVIOUS_STATUS_FIELD_OFFSET = EVENT_JOB_STATUS_JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int EVENT_JOB_STATUS_NEW_STATUS_FIELD_OFFSET = EVENT_JOB_STATUS_PREVIOUS_STATUS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_JOB_STATUS_USER_REQUESTED_FIELD_OFFSET = EVENT_JOB_STATUS_NEW_STATUS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EVENT_JOB_STATUS_INITIAL_FRAME_SIZE = EVENT_JOB_STATUS_USER_REQUESTED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    //hex: 0xFE1302
    private static final int EVENT_JOB_STATUS_MESSAGE_TYPE = 16651010;

    private JetAddJobStatusListenerCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Id of job.
         */
        public long jobId;

        /**
         * If true fires events that originated from this node only, otherwise fires all events.
         */
        public boolean localOnly;
    }

    public static ClientMessage encodeRequest(long jobId, boolean localOnly) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Jet.AddJobStatusListener");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET, jobId);
        encodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET, localOnly);
        clientMessage.add(initialFrame);
        return clientMessage;
    }

    public static JetAddJobStatusListenerCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.jobId = decodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET);
        request.localOnly = decodeBoolean(initialFrame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET);
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
     * A unique string which is used as a key to remove the listener.
     */
    public static java.util.UUID decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeUUID(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }

    public static ClientMessage encodeJobStatusEvent(long jobId, int previousStatus, int newStatus, @Nullable java.lang.String description, boolean userRequested) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[EVENT_JOB_STATUS_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        initialFrame.flags |= ClientMessage.IS_EVENT_FLAG;
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, EVENT_JOB_STATUS_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, EVENT_JOB_STATUS_JOB_ID_FIELD_OFFSET, jobId);
        encodeInt(initialFrame.content, EVENT_JOB_STATUS_PREVIOUS_STATUS_FIELD_OFFSET, previousStatus);
        encodeInt(initialFrame.content, EVENT_JOB_STATUS_NEW_STATUS_FIELD_OFFSET, newStatus);
        encodeBoolean(initialFrame.content, EVENT_JOB_STATUS_USER_REQUESTED_FIELD_OFFSET, userRequested);
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, description, StringCodec::encode);
        return clientMessage;
    }

    public abstract static class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
            if (messageType == EVENT_JOB_STATUS_MESSAGE_TYPE) {
                ClientMessage.Frame initialFrame = iterator.next();
                long jobId = decodeLong(initialFrame.content, EVENT_JOB_STATUS_JOB_ID_FIELD_OFFSET);
                int previousStatus = decodeInt(initialFrame.content, EVENT_JOB_STATUS_PREVIOUS_STATUS_FIELD_OFFSET);
                int newStatus = decodeInt(initialFrame.content, EVENT_JOB_STATUS_NEW_STATUS_FIELD_OFFSET);
                boolean userRequested = decodeBoolean(initialFrame.content, EVENT_JOB_STATUS_USER_REQUESTED_FIELD_OFFSET);
                java.lang.String description = CodecUtil.decodeNullable(iterator, StringCodec::decode);
                handleJobStatusEvent(jobId, previousStatus, newStatus, description, userRequested);
                return;
            }
            Logger.getLogger(super.getClass()).finest("Unknown message type received on event handler :" + messageType);
        }

        /**
         * @param jobId Id of job.
         * @param previousStatus NOT_RUNNING(0)
         *                       STARTING(1)
         *                       RUNNING(2)
         *                       SUSPENDED(3)
         *                       SUSPENDED_EXPORTING_SNAPSHOT(4)
         *                       FAILED(6)
         *                       COMPLETED(7)
         * @param newStatus See {@code previousStatus}.
         * @param description If the event is generated by the user, indicates the action, namely one
         *                    of SUSPEND, RESUME, RESTART and CANCEL; if there is a failure, indicates
         *                    the cause; otherwise, null.
         * @param userRequested Indicates whether the event is generated by the user via
         *                      {@code Job.suspend()}, {@code Job.resume()}, {@code Job.restart()} or
         *                      {@code Job.cancel()}.
         */
        public abstract void handleJobStatusEvent(long jobId, int previousStatus, int newStatus, @Nullable java.lang.String description, boolean userRequested);
    }
}

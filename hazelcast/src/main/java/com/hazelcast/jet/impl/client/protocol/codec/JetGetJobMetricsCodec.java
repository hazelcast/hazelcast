/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.client.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;
import com.hazelcast.client.impl.protocol.codec.custom.*;

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
 */
@Generated("8f56e259127236c4fb0817718c0e5781")
public final class JetGetJobMetricsCodec {
    //hex: 0xFE0D00
    public static final int REQUEST_MESSAGE_TYPE = 16649472;
    //hex: 0xFE0D01
    public static final int RESPONSE_MESSAGE_TYPE = 16649473;
    private static final int REQUEST_JOB_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_IS_LIGHT_JOB_FIELD_OFFSET = REQUEST_JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_IS_LIGHT_JOB_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private JetGetJobMetricsCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         */
        public long jobId;

        /**
         */
        public boolean isLightJob;

        /**
         * True if the isLightJob is received from the client, false otherwise.
         * If this is false, isLightJob has the default value for its type.
         */
        public boolean isIsLightJobExists;
    }

    public static ClientMessage encodeRequest(long jobId, boolean isLightJob) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("Jet.GetJobMetrics");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET, jobId);
        encodeBoolean(initialFrame.content, REQUEST_IS_LIGHT_JOB_FIELD_OFFSET, isLightJob);
        clientMessage.add(initialFrame);
        return clientMessage;
    }

    public static JetGetJobMetricsCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.jobId = decodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET);
        if (initialFrame.content.length >= REQUEST_IS_LIGHT_JOB_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES) {
            request.isLightJob = decodeBoolean(initialFrame.content, REQUEST_IS_LIGHT_JOB_FIELD_OFFSET);
            request.isIsLightJobExists = true;
        } else {
            request.isIsLightJobExists = false;
        }
        return request;
    }

    public static ClientMessage encodeResponse(com.hazelcast.internal.serialization.Data response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        DataCodec.encode(clientMessage, response);
        return clientMessage;
    }

    /**
     */
    public static com.hazelcast.internal.serialization.Data decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        //empty initial frame
        iterator.next();
        return DataCodec.decode(iterator);
    }

}

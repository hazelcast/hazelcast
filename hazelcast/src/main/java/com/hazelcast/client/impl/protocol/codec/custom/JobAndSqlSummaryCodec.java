/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil;
import com.hazelcast.client.impl.protocol.codec.builtin.CustomTypeFactory;
import com.hazelcast.client.impl.protocol.codec.builtin.StringCodec;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.LONG_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeLong;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeLong;

@Generated("db839997473838de8f00d99fb8160fb3")
public final class JobAndSqlSummaryCodec {
    private static final int LIGHT_JOB_FIELD_OFFSET = 0;
    private static final int JOB_ID_FIELD_OFFSET = LIGHT_JOB_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int EXECUTION_ID_FIELD_OFFSET = JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int SUBMISSION_TIME_FIELD_OFFSET = EXECUTION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int COMPLETION_TIME_FIELD_OFFSET = SUBMISSION_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = COMPLETION_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private JobAndSqlSummaryCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.jet.impl.JobAndSqlSummary jobAndSqlSummary) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, LIGHT_JOB_FIELD_OFFSET, jobAndSqlSummary.isLightJob());
        encodeLong(initialFrame.content, JOB_ID_FIELD_OFFSET, jobAndSqlSummary.getJobId());
        encodeLong(initialFrame.content, EXECUTION_ID_FIELD_OFFSET, jobAndSqlSummary.getExecutionId());
        encodeLong(initialFrame.content, SUBMISSION_TIME_FIELD_OFFSET, jobAndSqlSummary.getSubmissionTime());
        encodeLong(initialFrame.content, COMPLETION_TIME_FIELD_OFFSET, jobAndSqlSummary.getCompletionTime());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, jobAndSqlSummary.getNameOrId());
        JobStatusCodec.encode(clientMessage, jobAndSqlSummary.getStatus());
        CodecUtil.encodeNullable(clientMessage, jobAndSqlSummary.getFailureText(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, jobAndSqlSummary.getSqlSummary(), SqlSummaryCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.jet.impl.JobAndSqlSummary decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean lightJob = decodeBoolean(initialFrame.content, LIGHT_JOB_FIELD_OFFSET);
        long jobId = decodeLong(initialFrame.content, JOB_ID_FIELD_OFFSET);
        long executionId = decodeLong(initialFrame.content, EXECUTION_ID_FIELD_OFFSET);
        long submissionTime = decodeLong(initialFrame.content, SUBMISSION_TIME_FIELD_OFFSET);
        long completionTime = decodeLong(initialFrame.content, COMPLETION_TIME_FIELD_OFFSET);

        java.lang.String nameOrId = StringCodec.decode(iterator);
        com.hazelcast.jet.core.JobStatus status = JobStatusCodec.decode(iterator);
        java.lang.String failureText = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        com.hazelcast.jet.impl.SqlSummary sqlSummary = CodecUtil.decodeNullable(iterator, SqlSummaryCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createJobAndSqlSummary(lightJob, jobId, executionId, nameOrId, status, submissionTime, completionTime, failureText, sqlSummary);
    }
}

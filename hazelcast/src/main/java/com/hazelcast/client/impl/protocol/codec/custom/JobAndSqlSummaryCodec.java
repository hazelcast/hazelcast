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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@SuppressWarnings("unused")
@Generated("993d2740c510c057bd6dac4c2a78a257")
public final class JobAndSqlSummaryCodec {
    private static final int LIGHT_JOB_FIELD_OFFSET = 0;
    private static final int JOB_ID_FIELD_OFFSET = LIGHT_JOB_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int EXECUTION_ID_FIELD_OFFSET = JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int STATUS_FIELD_OFFSET = EXECUTION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int SUBMISSION_TIME_FIELD_OFFSET = STATUS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int COMPLETION_TIME_FIELD_OFFSET = SUBMISSION_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int USER_CANCELLED_FIELD_OFFSET = COMPLETION_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = USER_CANCELLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private JobAndSqlSummaryCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.jet.impl.JobAndSqlSummary jobAndSqlSummary) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, LIGHT_JOB_FIELD_OFFSET, jobAndSqlSummary.isLightJob());
        encodeLong(initialFrame.content, JOB_ID_FIELD_OFFSET, jobAndSqlSummary.getJobId());
        encodeLong(initialFrame.content, EXECUTION_ID_FIELD_OFFSET, jobAndSqlSummary.getExecutionId());
        encodeInt(initialFrame.content, STATUS_FIELD_OFFSET, jobAndSqlSummary.getStatus());
        encodeLong(initialFrame.content, SUBMISSION_TIME_FIELD_OFFSET, jobAndSqlSummary.getSubmissionTime());
        encodeLong(initialFrame.content, COMPLETION_TIME_FIELD_OFFSET, jobAndSqlSummary.getCompletionTime());
        encodeBoolean(initialFrame.content, USER_CANCELLED_FIELD_OFFSET, jobAndSqlSummary.isUserCancelled());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, jobAndSqlSummary.getNameOrId());
        CodecUtil.encodeNullable(clientMessage, jobAndSqlSummary.getFailureText(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, jobAndSqlSummary.getSqlSummary(), SqlSummaryCodec::encode);
        CodecUtil.encodeNullable(clientMessage, jobAndSqlSummary.getSuspensionCause(), StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.jet.impl.JobAndSqlSummary decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean lightJob = decodeBoolean(initialFrame.content, LIGHT_JOB_FIELD_OFFSET);
        long jobId = decodeLong(initialFrame.content, JOB_ID_FIELD_OFFSET);
        long executionId = decodeLong(initialFrame.content, EXECUTION_ID_FIELD_OFFSET);
        int status = decodeInt(initialFrame.content, STATUS_FIELD_OFFSET);
        long submissionTime = decodeLong(initialFrame.content, SUBMISSION_TIME_FIELD_OFFSET);
        long completionTime = decodeLong(initialFrame.content, COMPLETION_TIME_FIELD_OFFSET);
        boolean isUserCancelledExists = false;
        boolean userCancelled = false;
        if (initialFrame.content.length >= USER_CANCELLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES) {
            userCancelled = decodeBoolean(initialFrame.content, USER_CANCELLED_FIELD_OFFSET);
            isUserCancelledExists = true;
        }

        java.lang.String nameOrId = StringCodec.decode(iterator);
        java.lang.String failureText = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        com.hazelcast.jet.impl.SqlSummary sqlSummary = CodecUtil.decodeNullable(iterator, SqlSummaryCodec::decode);
        boolean isSuspensionCauseExists = false;
        java.lang.String suspensionCause = null;
        if (!iterator.peekNext().isEndFrame()) {
            suspensionCause = CodecUtil.decodeNullable(iterator, StringCodec::decode);
            isSuspensionCauseExists = true;
        }

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createJobAndSqlSummary(lightJob, jobId, executionId, nameOrId, status, submissionTime, completionTime, failureText, sqlSummary, isSuspensionCauseExists, suspensionCause, isUserCancelledExists, userCancelled);
    }
}

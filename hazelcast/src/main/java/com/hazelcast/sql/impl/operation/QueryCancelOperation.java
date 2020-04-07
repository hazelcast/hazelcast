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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.UUID;

/**
 * Operation to cancel query execution on participant node. Cancellation process is two-phase:
 * 1) When a participant would like to cancel the query, this request is sent to the query initiator
 * 2) When initiator receives cancel request from a participant (including self), it broadcasts it to
 *    other participants.
 *
 * This two-phase process is required to minimize the number of messages passed between nodes. Consider a query cancel due to
 * timeout. If only broadcast is used from participants, then every participant will send a message to all other participants,
 * leading to N*N messages in the worst case. With two-phase approach query initiator ensures that broadcast is performed no
 * more than once, putting upper bound on the total number of cancel messages to 2*N.
 */
public class QueryCancelOperation extends QueryAbstractIdAwareOperation {

    private int errorCode;
    private String errorMessage;
    private UUID originatingMemberId;

    public QueryCancelOperation() {
        // No-op.
    }

    public QueryCancelOperation(QueryId queryId, int errorCode, String errorMessage, UUID originatingMemberId) {
        super(queryId);

        assert originatingMemberId != null;

        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.originatingMemberId = originatingMemberId;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public UUID getOriginatingMemberId() {
        return originatingMemberId;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.OPERATION_CANCEL;
    }

    @Override
    protected void writeInternal1(ObjectDataOutput out) throws IOException {
        out.writeInt(errorCode);
        out.writeUTF(errorMessage);
        UUIDSerializationUtil.writeUUID(out, originatingMemberId);
    }

    @Override
    protected void readInternal1(ObjectDataInput in) throws IOException {
        errorCode = in.readInt();
        errorMessage = in.readUTF();
        originatingMemberId = UUIDSerializationUtil.readUUID(in);
    }
}

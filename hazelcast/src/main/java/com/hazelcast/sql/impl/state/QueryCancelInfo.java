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

package com.hazelcast.sql.impl.state;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.UUID;

/**
 * Query cancel info.
 */
public class QueryCancelInfo implements DataSerializable {
    /** Member which triggered cancel. */
    private UUID originatingMemberId;

    /** Error code. See {@link com.hazelcast.sql.SqlErrorCode} */
    private int errorCode;

    /** Error message (optional). */
    private String errorMessage;

    public QueryCancelInfo() {
        // No-op.
    }

    public QueryCancelInfo(UUID originatingMemberId, int errorCode) {
        this(originatingMemberId, errorCode, null);
    }

    public QueryCancelInfo(UUID originatingMemberId, int errorCode, String errorMessage) {
        this.originatingMemberId = originatingMemberId;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public UUID getOriginatingMemberId() {
        return originatingMemberId;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, originatingMemberId);
        out.writeInt(errorCode);
        out.writeUTF(errorMessage);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        originatingMemberId = UUIDSerializationUtil.readUUID(in);
        errorCode = in.readInt();
        errorMessage = in.readUTF();
    }

    @Override
    public String toString() {
        return "QueryCancelInfo{originatingMemberId=" + originatingMemberId + ", errorCode=" + errorCode
                   + ", errorMessage=" + errorMessage + '}';
    }
}

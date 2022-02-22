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

package com.hazelcast.spi.impl.operationservice.impl.responses;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * A {@link Response} is a result of an {@link Operation} being executed.
 * There are different types of responses:
 * <ol>
 * <li>{@link NormalResponse} the result of a regular Operation result, e.g. Map.put()</li>
 * <li>{@link BackupAckResponse} the result of a completed
 * {@link com.hazelcast.spi.impl.operationservice.impl.operations.Backup}</li>
 * </ol>
 */
public abstract class Response implements IdentifiedDataSerializable {

    public static final int OFFSET_SERIALIZER_TYPE_ID = 4;
    public static final int OFFSET_IDENTIFIED = OFFSET_SERIALIZER_TYPE_ID + INT_SIZE_IN_BYTES;
    public static final int OFFSET_TYPE_FACTORY_ID = OFFSET_IDENTIFIED + 1;
    public static final int OFFSET_TYPE_ID = OFFSET_TYPE_FACTORY_ID + INT_SIZE_IN_BYTES;
    public static final int OFFSET_CALL_ID = OFFSET_TYPE_ID + INT_SIZE_IN_BYTES;
    public static final int OFFSET_URGENT = OFFSET_CALL_ID + LONG_SIZE_IN_BYTES;
    public static final int RESPONSE_SIZE_IN_BYTES = OFFSET_URGENT + 1;

    protected long callId;
    protected boolean urgent;

    public Response() {
    }

    public Response(long callId, boolean urgent) {
        this.callId = callId;
        this.urgent = urgent;
    }

    /**
     * Check if this Response is an urgent response.
     *
     * @return {@code true} if urgent, {@code false} otherwise
     */
    public boolean isUrgent() {
        return urgent;
    }

    /**
     * Returns the call ID of the operation this response belongs to.
     *
     * @return the call ID
     */
    public long getCallId() {
        return callId;
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(callId);
        out.writeBoolean(urgent);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        callId = in.readLong();
        urgent = in.readBoolean();
    }
}

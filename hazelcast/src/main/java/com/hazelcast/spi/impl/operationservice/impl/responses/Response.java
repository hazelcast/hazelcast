/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;

/**
 * A {@link Response} is a result of an {@link com.hazelcast.spi.Operation} being executed.
 * There are different types of responses:
 */
public abstract class Response implements IdentifiedDataSerializable {

    protected long callId;
    protected boolean urgent;
    private final int id;

    public Response(int id) {
        this.id = id;
    }

    public Response(int id, long callId, boolean urgent) {
        this.id = id;
        this.callId = callId;
        this.urgent = urgent;
    }

    /**
     * Check if this Response is an urgent response.
     *
     * @return true if urgent, false otherwise.
     */
    public final boolean isUrgent() {
        return urgent;
    }

    /**
     * Returns the call id of the operation this response belongs to.
     *
     * @return the call id.
     */
    public final long getCallId() {
        return callId;
    }

    @Override
    public final int getId() {
        return id;
    }

    @Override
    public final int getFactoryId() {
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

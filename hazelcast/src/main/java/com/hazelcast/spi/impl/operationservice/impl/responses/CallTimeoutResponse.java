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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;

import static com.hazelcast.spi.impl.SpiDataSerializerHook.CALL_TIMEOUT_RESPONSE;

/**
 * An response that indicates that the execution of a single call ran into a timeout.
 */
public class CallTimeoutResponse extends Response implements IdentifiedDataSerializable {

    public CallTimeoutResponse() {
    }

    public CallTimeoutResponse(long callId, boolean urgent) {
        super(callId, urgent);
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CALL_TIMEOUT_RESPONSE;
    }

    @Override
    public String toString() {
        return "CallTimeoutResponse{callId=" + callId + ", urgent=" + urgent + '}';
    }
}


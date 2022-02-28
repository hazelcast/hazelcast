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

import java.io.IOException;

import static com.hazelcast.spi.impl.SpiDataSerializerHook.ERROR_RESPONSE;

public class ErrorResponse extends Response {
    private Throwable cause;

    public ErrorResponse() {
    }

    public ErrorResponse(Throwable cause, long callId, boolean urgent) {
        super(callId, urgent);
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    @Override
    public int getClassId() {
        return ERROR_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(cause);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        cause = in.readObject();
    }

    @Override
    public String toString() {
        return "ErrorResponse{callId=" + callId + ", urgent=" + urgent + ", cause=" + cause + '}';
    }
}

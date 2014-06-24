/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class ClientResponse implements IdentifiedDataSerializable {

    private Data response;
    private int callId;
    private boolean isEvent;
    private boolean isError;

    public ClientResponse() {
    }

    public ClientResponse(Data response, int callId, boolean isError, boolean isEvent) {
        this.response = response;
        this.callId = callId;
        this.isError = isError;
        this.isEvent = isEvent;
    }

    public Data getResponse() {
        return response;
    }

    public int getCallId() {
        return callId;
    }

    public boolean isEvent() {
        return isEvent;
    }

    public boolean isError() {
        return isError;
    }

    @Override
    public int getFactoryId() {
        return ClientDataSerializerHook.ID;
    }

    @Override
    public int getId() {
        return ClientDataSerializerHook.CLIENT_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(callId);
        out.writeBoolean(isEvent);
        out.writeBoolean(isError);
        response.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        callId = in.readInt();
        isEvent = in.readBoolean();
        isError = in.readBoolean();
        response = new Data();
        response.readData(in);
    }
}

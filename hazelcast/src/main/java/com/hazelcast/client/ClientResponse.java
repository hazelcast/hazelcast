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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * @author ali 20/12/13
 */
public class ClientResponse implements IdentifiedDataSerializable {

    Object response;

    long callId;

    public ClientResponse() {
    }

    public ClientResponse(Object response, long callId) {
        this.response = response;
        this.callId = callId;
    }

    public Object getResponse() {
        return response;
    }

    public long getCallId() {
        return callId;
    }

    public int getFactoryId() {
        return ClientDataSerializerHook.ID;
    }

    public int getId() {
        return ClientDataSerializerHook.CLIENT_RESPONSE;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(callId);
        out.writeObject(response);
    }

    public void readData(ObjectDataInput in) throws IOException {
        callId = in.readLong();
        response = in.readObject();
    }
}

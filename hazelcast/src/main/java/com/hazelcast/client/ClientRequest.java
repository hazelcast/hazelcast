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

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 *
 * @author mdogan 4/29/13
 */
public abstract class ClientRequest implements Portable {

    int callId = -1;

    transient ClientEngineImpl clientEngine;

    transient Object service;

    transient ClientEndpoint endpoint;

    abstract void process() throws Exception;

    public ClientEngine getClientEngine() {
        return clientEngine;
    }

    final void setClientEngine(ClientEngineImpl clientEngine) {
        this.clientEngine = clientEngine;
    }

    public <S> S getService() {
        return (S) service;
    }

    final void setService(Object service) {
        this.service = service;
    }

    public ClientEndpoint getEndpoint() {
        return endpoint;
    }

    final void setEndpoint(ClientEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public abstract String getServiceName();

    public int getCallId() {
        return callId;
    }

    public void setCallId(int callId) {
        this.callId = callId;
    }

    public final void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("cId", callId);
        write(writer);
    }

    public  void write(PortableWriter writer) throws IOException {

    }

    public final void readPortable(PortableReader reader) throws IOException {
        callId = reader.readInt("cId");
        read(reader);
    }

    public void read(PortableReader reader) throws IOException {

    }
}

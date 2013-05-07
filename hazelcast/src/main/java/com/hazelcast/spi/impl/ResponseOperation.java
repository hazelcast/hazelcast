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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;

final class ResponseOperation extends Operation implements IdentifiedDataSerializable {

    private Object result;
    private boolean exception = false;
    private int backupCount;

    public ResponseOperation() {
    }

    ResponseOperation(Object result) {
        if (result instanceof Response) {
            final Response response = (Response) result;
            this.result = response.response;
            this.backupCount = response.backupCount;
        } else {
            this.result = result;
        }
        this.exception = result instanceof Throwable;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        if (exception) {
            result = nodeEngine.toObject(result);
        }
        nodeEngine.operationService.notifyRemoteCall(getCallId(), getResponse());
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    public Object getResponse() {
        return exception ? result : new Response(result, getCallId(), backupCount);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        final boolean isData = result instanceof Data;
        out.writeBoolean(isData);
        if (isData) {
            ((Data) result).writeData(out);
        } else {
            out.writeObject(result);
        }
        out.writeBoolean(exception);
        out.writeInt(backupCount);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        final boolean isData = in.readBoolean();
        if (isData) {
            Data data = new Data();
            data.readData(in);
            result = data;
        } else {
            result = in.readObject();
        }
        exception = in.readBoolean();
        backupCount = in.readInt();
    }

    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    public int getId() {
        return SpiDataSerializerHook.RESPONSE;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResponseOperation{");
        sb.append("result=").append(result);
        sb.append(", exception=").append(exception);
        sb.append(", backupCount=").append(backupCount);
        sb.append('}');
        return sb.toString();
    }
}

/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.AbstractOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.toObject;

public final class Response extends AbstractOperation {

    private Object result = null;
    private boolean exception = false;

    public Response() {
    }

    public Response(Object result) {
        this(result, (result instanceof Throwable));
    }

    public Response(Object result, boolean exception) {
        this.result = result;
        this.exception = exception;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        final NodeServiceImpl nodeService = (NodeServiceImpl) getNodeService();
        final long callId = getCallId();
        final Object response;
        if (exception) {
            response = toObject(result);
        } else {
            response = result;
        }
        nodeService.notifyCall(callId, response);
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean needsBackup() {
        return false;
    }

    public boolean isException() {
        return exception;
    }

    public Object getResult() {
        return result;
    }

    public void writeInternal(DataOutput out) throws IOException {
        final boolean isData = result instanceof Data;
        out.writeBoolean(isData);
        if (isData) {
            ((Data) result).writeData(out);
        } else {
            IOUtil.writeObject(out, result);
        }
        out.writeBoolean(exception);
    }

    public void readInternal(DataInput in) throws IOException {
        final boolean isData = in.readBoolean();
        if (isData) {
            Data data = new Data();
            data.readData(in);
            result = data;
        } else {
            result = IOUtil.readObject(in);
        }
        exception = in.readBoolean();
    }

    @Override
    public String toString() {
        return "Response{" +
               "result=" + getResult() +
               ", exception=" + exception +
               '}';
    }
}

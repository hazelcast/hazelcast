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

package com.hazelcast.impl.spi;

import com.hazelcast.nio.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class Response extends AbstractOperation implements NonBlockingOperation, NoReply {
    Object result = null;
    Data resultData = null;
    boolean exception = false;

    public Response() {
    }

    public Response(Object result) {
        this(result, (result instanceof Throwable));
    }

    public Response(Object result, boolean exception) {
        this.result = result;
        this.exception = exception;
        try {
            this.resultData = toData(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        long callId = getOperationContext().getCallId();
        getOperationContext().getNodeService().notifyCall(callId, Response.this);
    }

    public boolean isException() {
        return exception;
    }

    public Data getResultData() {
        return resultData;
    }

    public Object getResult() {
        if (result == null) {
            result = toObject(resultData);
        }
        return result;
    }

    public void writeData(DataOutput out) throws IOException {
        boolean NULL = (resultData == null);
        out.writeBoolean(NULL);
        if (!NULL) {
            resultData.writeData(out);
        }
        out.writeBoolean(exception);
    }

    public void readData(DataInput in) throws IOException {
        boolean NULL = in.readBoolean();
        if (!NULL) {
            resultData = new Data();
            resultData.readData(in);
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

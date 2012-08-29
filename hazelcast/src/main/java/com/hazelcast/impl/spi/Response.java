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
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class Response extends Operation implements NoReply {
    private Object result = null;
    private Data resultData = null;
    private boolean exception = false;
    private Data opBeforeData = null;

    public Response() {
    }

    public Response(Object result) {
        this(result, (result instanceof Throwable));
    }

    public Response(Object result, boolean exception) {
        this(null, result, exception);
    }

    public Response(Operation opBefore, Object result, boolean exception) {
        this.result = result;
        this.exception = exception;
        try {
            this.resultData = toData(result);
            this.opBeforeData = toData(opBefore);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        if (opBeforeData != null) {
            Operation op = (Operation) toObject(opBeforeData);
            op.setCallId(getCallId())
                    .setService(getService())
                    .setPartitionId(getPartitionId())
                    .setCaller(getCaller())
                    .setCallId(getCallId())
                    .setNodeService(getNodeService());
            op.run();
        }
        long callId = getCallId();
        final NodeServiceImpl nodeService = (NodeServiceImpl) getNodeService();
        nodeService.notifyCall(callId, Response.this);
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

    public void writeInternal(DataOutput out) throws IOException {
        IOUtil.writeNullableData(out, opBeforeData);
        IOUtil.writeNullableData(out, resultData);
        out.writeBoolean(exception);
    }

    public void readInternal(DataInput in) throws IOException {
        opBeforeData = IOUtil.readNullableData(in);
        resultData = IOUtil.readNullableData(in);
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

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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.*;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @mdogan 10/1/12
 */

public final class MultiResponse extends AbstractOperation implements ResponseOperation, IdentifiedDataSerializable {

    private Data[] operationData;

    private transient Operation[] operations;

    private transient boolean hasResponse = false;

    public MultiResponse() {
    }

    public MultiResponse(SerializationService serializationService, final Object... responses) {
        operationData = new Data[responses.length];
        for (int i = 0; i < responses.length; i++) {
            /*if (responses[i] instanceof MultiResponse) {
                MultiResponse mr = (MultiResponse) responses[i];
                if (mr.hasResponse) {
                    markHasResponse();
                }
                Data[] newOperationData = new Data[operationData.length + mr.operationData.length - 1];
                System.arraycopy(operationData, 0, newOperationData, 0, i);
                System.arraycopy(mr.operationData, 0, newOperationData, i, mr.operationData.length);
                operationData = newOperationData;
            }
            else */if (responses[i] instanceof Operation) {
                if (responses[i] instanceof Response) {
                    markHasResponse();
                }
                operationData[i] = serializationService.toData(responses[i]);
            } else {
                markHasResponse();
                operationData[i] = serializationService.toData(new Response(responses[i]));
            }
        }
    }

    private void markHasResponse() {
        if (hasResponse) {
            throw new IllegalArgumentException("Only and only one Response operation can be sent at a time!");
        }
        hasResponse = true;
    }

    public void beforeRun() throws Exception {
        final NodeEngine nodeEngine = getNodeEngine();
        final int len = operationData.length;
        operations = new Operation[len];
        for (int i = 0; i < len; i++) {
            final Operation op = (Operation) nodeEngine.toObject(operationData[i]);
            op.setNodeEngine(nodeEngine)
                    .setResponseHandler(new ResponseHandler() {
                        public void sendResponse(Object obj) {
                            if (obj instanceof Throwable) {
                                final Throwable t = (Throwable) obj;
                                final ILogger logger = nodeEngine.getLogger(op.getClass());
                                logger.log(Level.WARNING, "While executing operation within MultiResponse: " + t.getMessage(), t);
                            }
                        }
                    });
            OperationAccessor.setCallerAddress(op, getCallerAddress());
            OperationAccessor.setConnection(op, getConnection());
            if (op instanceof Response) {
                OperationAccessor.setCallId(op, getCallId());
            }
            operations[i] = op;
        }
    }

    public void run() throws Exception {
        if (operations != null && operations.length > 0) {
            final NodeEngine nodeEngine = getNodeEngine();
            for (final Operation op : operations) {
                if (op != null) {
                    nodeEngine.getOperationService().runOperation(op);
                }
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        int len = operationData.length;
        out.writeInt(len);
        for (Data op : operationData) {
            op.writeData(out);
        }
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        int len = in.readInt();
        operationData = new Data[len];
        for (int i = 0; i < len; i++) {
            operationData[i] = new Data();
            operationData[i].readData(in);
        }
    }

    public int getId() {
        return DataSerializerSpiHook.MULTI_RESPONSE;
    }
}

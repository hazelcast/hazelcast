/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.ResponseHandlerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @mdogan 12/24/12
 */
public class PostJoinOperation extends AbstractOperation implements JoinOperation {

    private Data[] operationData;

    private transient Operation[] operations;

    public PostJoinOperation() {
    }

    public PostJoinOperation(final Operation... ops) {
        for (int i = 0; i < ops.length; i++) {
            Operation op = ops[i];
            if (op == null) {
                throw new NullPointerException();
            }
            if (op instanceof PartitionAwareOperation) {
                throw new IllegalArgumentException("Post join operation can not be a PartitionAwareOperation!");
            }
        }
        operations = ops; // we may need to do array copy!
    }

    @Override
    public void beforeRun() throws Exception {
        if (operations == null && operationData != null) {
            final NodeEngine nodeEngine = getNodeEngine();
            final int len = operationData.length;
            operations = new Operation[len];
            for (int i = 0; i < len; i++) {
                final Operation op = (Operation) nodeEngine.toObject(operationData[i]);
                op.setNodeEngine(nodeEngine)
                        .setCaller(getCaller())
                        .setConnection(getConnection())
                        .setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                OperationAccessor.setCallId(op, getCallId());
                operations[i] = op;
            }
        }
    }

    public void run() throws Exception {
        if (operations != null && operations.length > 0) {
            final NodeEngine nodeEngine = getNodeEngine();
            for (final Operation op : operations) {
                nodeEngine.getOperationService().runOperation(op);
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

//    @Override
//    public Object getResponse() {
//        return Boolean.TRUE;
//    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    private void prepareToWrite() {
        if (operationData == null && operations != null) {
            int len = operations.length;
            operationData = new Data[len];
            for (int i = 0; i < len; i++) {
                operationData[i] = IOUtil.toData(operations[i]);
            }
        }
    }

    @Override
    protected void writeInternal(final DataOutput out) throws IOException {
        prepareToWrite();
        int len = operationData.length;
        out.writeInt(len);
        for (Data op : operationData) {
            op.writeData(out);
        }
    }

    @Override
    protected void readInternal(final DataInput in) throws IOException {
        int len = in.readInt();
        operationData = new Data[len];
        for (int i = 0; i < len; i++) {
            operationData[i] = new Data();
            operationData[i].readData(in);
        }
    }
}

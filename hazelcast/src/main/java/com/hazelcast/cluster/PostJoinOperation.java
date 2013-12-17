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

package com.hazelcast.cluster;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.ResponseHandlerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author mdogan 12/24/12
 */
public class PostJoinOperation extends AbstractOperation implements UrgentSystemOperation {

    private transient Operation[] operations;

    public PostJoinOperation() {
    }

    public PostJoinOperation(final Operation... ops) {
        for (Operation op : ops) {
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
        if (operations != null && operations.length > 0) {
            final NodeEngine nodeEngine = getNodeEngine();
            final int len = operations.length;
            for (int i = 0; i < len; i++) {
                final Operation op = operations[i];
                op.setNodeEngine(nodeEngine).setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                OperationAccessor.setCallerAddress(op, getCallerAddress());
                OperationAccessor.setConnection(op, getConnection());
                operations[i] = op;
            }
        }
    }

    public void run() throws Exception {
        if (operations != null && operations.length > 0) {
            final OperationService operationService = getNodeEngine().getOperationService();
            for (final Operation op : operations) {
                operationService.runOperation(op);
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        final int len = operations != null ? operations.length : 0;
        out.writeInt(len);
        if (len > 0) {
            for (Operation op : operations) {
                out.writeObject(op);
            }
        }
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        final int len = in.readInt();
        operations = new Operation[len];
        for (int i = 0; i < len; i++) {
            operations[i] = in.readObject();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PostJoinOperation{");
        sb.append("operations=").append(Arrays.toString(operations));
        sb.append('}');
        return sb.toString();
    }
}

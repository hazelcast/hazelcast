/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.UrgentSystemOperation;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createErrorLoggingResponseHandler;
import static com.hazelcast.util.Preconditions.checkNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class PostJoinOp
        extends AbstractJoinOperation implements UrgentSystemOperation {

    private Operation[] operations;

    public PostJoinOp() {
    }

    public PostJoinOp(final Operation... ops) {
        for (Operation op : ops) {
            checkNotNull(op, "op can't be null");
            checkNegative(op.getPartitionId(), "Post join operation can not have a partition-id!");
        }
        // we may need to do array copy!
        operations = ops;
    }

    @Override
    public void beforeRun() throws Exception {
        if (operations != null && operations.length > 0) {
            final NodeEngine nodeEngine = getNodeEngine();
            final int len = operations.length;
            OperationResponseHandler responseHandler = createErrorLoggingResponseHandler(getLogger());
            for (int i = 0; i < len; i++) {
                final Operation op = operations[i];
                op.setNodeEngine(nodeEngine);
                op.setOperationResponseHandler(responseHandler);

                OperationAccessor.setCallerAddress(op, getCallerAddress());
                OperationAccessor.setConnection(op, getConnection());
                operations[i] = op;
            }
        }
    }

    @Override
    public void run() throws Exception {
        if (operations != null && operations.length > 0) {
            for (Operation op : operations) {
                try {
                    // not running via OperationService since we don't want any restrictions like cluster state check etc.
                    op.beforeRun();
                    op.run();
                    op.afterRun();
                } catch (Exception e) {
                    getLogger().warning("Error while running post-join operation: " + op, e);
                }
            }
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        if (operations != null) {
            for (Operation op : operations) {
                onOperationFailure(op, e);
            }
        }
    }

    private void onOperationFailure(Operation op, Throwable e) {
        try {
            op.onExecutionFailure(e);
        } catch (Throwable t) {
            getLogger().warning("While calling operation.onFailure(). op: " + op, t);
        }
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
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", operations=").append(Arrays.toString(operations));
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.POST_JOIN;
    }
}

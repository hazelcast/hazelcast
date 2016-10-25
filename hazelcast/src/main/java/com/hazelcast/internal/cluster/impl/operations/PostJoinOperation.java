/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.UrgentSystemOperation;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;

import static com.hazelcast.util.Preconditions.checkNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class PostJoinOperation extends AbstractJoinOperation implements UrgentSystemOperation {

    private Operation[] operations;

    public PostJoinOperation() {
    }

    public PostJoinOperation(final Operation... ops) {
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
            for (int i = 0; i < len; i++) {
                final Operation op = operations[i];
                op.setNodeEngine(nodeEngine);
                op.setOperationResponseHandler(new OperationResponseHandler() {
                    @Override
                    public void sendResponse(Operation op, Object obj) {
                        if (obj instanceof Throwable) {
                            Throwable t = (Throwable) obj;
                            ILogger logger = nodeEngine.getLogger(op.getClass());
                            logger.warning("Error while running post-join operation: "
                                    + t.getClass().getSimpleName() + ": " + t.getMessage());

                            if (logger.isFineEnabled()) {
                                logger.log(Level.FINE, "Error while running post-join operation: ", t);
                            }
                        }
                    }
                });

                OperationAccessor.setCallerAddress(op, getCallerAddress());
                OperationAccessor.setConnection(op, getConnection());
                operations[i] = op;
            }
        }
    }

    @Override
    public void run() throws Exception {
        if (operations != null && operations.length > 0) {
            for (final Operation op : operations) {
                op.beforeRun();
                op.run();
                op.afterRun();
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

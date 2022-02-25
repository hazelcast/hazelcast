/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Member;
import com.hazelcast.config.OnJoinPermissionOperationName;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.management.operation.UpdatePermissionConfigOperation;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.TargetAware;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;
import static com.hazelcast.spi.impl.operationexecutor.OperationRunner.runDirect;
import static com.hazelcast.spi.impl.operationservice.OperationResponseHandlerFactory.createErrorLoggingResponseHandler;
import static com.hazelcast.internal.util.Preconditions.checkNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class OnJoinOp
        extends AbstractJoinOperation implements UrgentSystemOperation, TargetAware {

    private Collection<Operation> operations;

    public OnJoinOp() {
    }

    public OnJoinOp(Collection<Operation> ops) {
        for (Operation op : ops) {
            checkNotNull(op, "op can't be null");
            checkNegative(op.getPartitionId(), "Post join operation can not have a partition ID!");
        }
        operations = ops;
    }

    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    public void beforeRun() throws Exception {
        if (!operations.isEmpty()) {
            NodeEngine nodeEngine = getNodeEngine();
            OperationResponseHandler responseHandler = createErrorLoggingResponseHandler(getLogger());
            for (Operation op : operations) {
                op.setNodeEngine(nodeEngine);
                op.setOperationResponseHandler(responseHandler);
                OperationAccessor.setCallerAddress(op, getCallerAddress());
                OperationAccessor.setConnection(op, getConnection());
            }
        }
    }

    @Override
    public void run() throws Exception {
        if (!operations.isEmpty()) {
            SecurityConfig securityConfig = getNodeEngine().getConfig().getSecurityConfig();
            boolean runPermissionUpdates = securityConfig.getOnJoinPermissionOperation() == OnJoinPermissionOperationName.RECEIVE;
            for (Operation op : operations) {
                if ((op instanceof UpdatePermissionConfigOperation) && !runPermissionUpdates) {
                    continue;
                }
                try {
                    // not running via OperationService since we don't want any restrictions like cluster state check etc.
                    runDirect(op);
                } catch (Exception e) {
                    getLogger().warning("Error while running post-join operation: " + op, e);
                }
            }

            final ClusterService clusterService = getService();
            // if executed on master, broadcast to all other members except sender (joining member)
            if (clusterService.isMaster()) {
                final OperationService operationService = getNodeEngine().getOperationService();
                for (Member member : clusterService.getMembers()) {
                    if (!member.localMember() && !member.getUuid().equals(getCallerUuid())) {
                        OnJoinOp operation = new OnJoinOp(operations);
                        operationService.invokeOnTarget(getServiceName(), operation, member.getAddress());
                    }
                }
            }
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        for (Operation op : operations) {
            onOperationFailure(op, e);
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
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        writeCollection(operations, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        operations = readCollection(in);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", operations=").append(operations);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.POST_JOIN;
    }

    @Override
    public void setTarget(Address address) {
        for (Operation op : operations) {
            if (op instanceof TargetAware) {
                ((TargetAware) op).setTarget(address);
            }
        }
    }
}

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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientDeployClassesCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentService;
import com.hazelcast.internal.usercodedeployment.impl.operation.DeployClassesOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.UserCodeDeploymentPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Message task handling user code deployment from client to cluster
 */
public class DeployClassesMessageTask extends AbstractMultiTargetMessageTask<List<Map.Entry<String, byte[]>>>
        implements Supplier<Operation> {

    public DeployClassesMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public Operation get() {
        return new DeployClassesOperation(parameters);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return this;
    }

    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        for (Object result : map.values()) {
            if (result instanceof Throwable && !(result instanceof MemberLeftException)) {
                throw (Throwable) result;
            }
        }
        return null;
    }

    @Override
    public Collection<Member> getTargets() {
        return nodeEngine.getClusterService().getMembers();
    }

    @Override
    protected List<Map.Entry<String, byte[]>> decodeClientMessage(ClientMessage clientMessage) {
        return ClientDeployClassesCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientDeployClassesCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return UserCodeDeploymentService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new UserCodeDeploymentPermission(ActionConstants.ACTION_USER_CODE_DEPLOY);
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}

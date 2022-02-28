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
import com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.proxyservice.impl.operations.DistributedObjectDestroyOperation;

import java.security.Permission;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;

import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.security.permission.ActionConstants.getPermission;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

public class DestroyProxyMessageTask extends AbstractMultiTargetMessageTask<ClientDestroyProxyCodec.RequestParameters>
        implements Supplier<Operation> {

    public DestroyProxyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public Operation get() {
        return new DistributedObjectDestroyOperation(parameters.serviceName, parameters.name);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return this;
    }

    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        for (Object result : map.values()) {
            if (result instanceof Throwable) {
                handleException((Throwable) result);
            }
        }
        return null;
    }

    @Override
    public Collection<Member> getTargets() {
        return nodeEngine.getClusterService().getMembers();
    }

    private void handleException(Throwable throwable) {
        boolean causedByInactiveInstance = peel(throwable) instanceof HazelcastInstanceNotActiveException;
        Level level = causedByInactiveInstance ? FINEST : WARNING;
        logger.log(level, "Error while destroying a proxy.", throwable);
    }

    @Override
    protected ClientDestroyProxyCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientDestroyProxyCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientDestroyProxyCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return parameters.serviceName;
    }

    @Override
    public Permission getRequiredPermission() {
        return getPermission(parameters.name, parameters.serviceName, ActionConstants.ACTION_DESTROY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}

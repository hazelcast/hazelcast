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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.impl.querycache.subscriber.operation.DestroyQueryCacheOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec#REQUEST_MESSAGE_TYPE}
 */
public class MapDestroyCacheMessageTask
        extends AbstractMultiTargetMessageTask<ContinuousQueryDestroyCacheCodec.RequestParameters>
        implements Supplier<Operation> {

    public MapDestroyCacheMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return this;
    }

    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        return !map.values().contains(Boolean.FALSE);
    }

    @Override
    public Collection<Member> getTargets() {
        return clientEngine.getClusterService().getMembers();
    }

    @Override
    protected ContinuousQueryDestroyCacheCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ContinuousQueryDestroyCacheCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ContinuousQueryDestroyCacheCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
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
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Operation get() {
        DestroyQueryCacheOperation operation = new DestroyQueryCacheOperation(parameters.mapName, parameters.cacheName);
        operation.setCallerUuid(endpoint.getUuid());
        return operation;
    }
}

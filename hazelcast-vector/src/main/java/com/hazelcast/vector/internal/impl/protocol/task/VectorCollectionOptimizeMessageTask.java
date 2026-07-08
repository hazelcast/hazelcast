/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionOptimizeCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.VectorCollectionPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.vector.internal.impl.ops.OptimizeOperationsFactory;
import com.hazelcast.vector.internal.impl.stats.LocalVectorCollectionStatsImpl;

import java.security.Permission;
import java.util.Map;

import static com.hazelcast.security.permission.ActionConstants.ACTION_OPTIMIZE;

public class VectorCollectionOptimizeMessageTask
    extends AbstractVectorCollectionAllPartitionsMessageTask<VectorCollectionOptimizeCodec.RequestParameters> {

    VectorCollectionOptimizeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected VectorCollectionOptimizeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return VectorCollectionOptimizeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return VectorCollectionOptimizeCodec.encodeResponse();
    }

    @Override
    protected OperationFactory createOperationFactory() {
        var factory = new OptimizeOperationsFactory(parameters.name, parameters.indexName);
        if (parameters.uuid != null) {
            factory.setUuid(parameters.uuid);
        }
        return factory;
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        recordStats(LocalVectorCollectionStatsImpl::incrementOptimizeLatencyNanos);
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.OPTIMIZE;
    }

    @Override
    public Permission getRequiredPermission() {
        return new VectorCollectionPermission(getDistributedObjectName(), ACTION_OPTIMIZE);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.indexName, parameters.uuid};
    }
}

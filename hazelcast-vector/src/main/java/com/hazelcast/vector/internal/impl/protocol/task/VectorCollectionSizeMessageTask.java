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
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSizeCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.VectorCollectionPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.vector.internal.impl.ops.SizeOperationsFactory;
import com.hazelcast.vector.internal.impl.stats.LocalVectorCollectionStatsImpl;

import java.security.Permission;
import java.util.Map;

import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;

public class VectorCollectionSizeMessageTask
    extends AbstractVectorCollectionAllPartitionsMessageTask<String> {

    VectorCollectionSizeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return VectorCollectionSizeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return VectorCollectionSizeCodec.encodeResponse((long) response);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new SizeOperationsFactory(parameters);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        long sum = map.values().stream().mapToLong(object -> (long) object).sum();

        recordStats(LocalVectorCollectionStatsImpl::incrementSizeLatencyNanos);

        return sum;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.SIZE;
    }

    @Override
    public Permission getRequiredPermission() {
        return new VectorCollectionPermission(getDistributedObjectName(), ACTION_READ);
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}

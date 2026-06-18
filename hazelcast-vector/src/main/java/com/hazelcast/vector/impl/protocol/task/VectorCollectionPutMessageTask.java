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

package com.hazelcast.vector.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.VectorCollectionPermission;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.impl.VectorUtil;
import com.hazelcast.vector.impl.ops.PutOperation;
import com.hazelcast.vector.impl.stats.LocalVectorCollectionStatsImpl;

import java.security.Permission;

import static com.hazelcast.security.permission.ActionConstants.ACTION_PUT;

public class VectorCollectionPutMessageTask
        extends AbstractVectorCollectionPartitionMessageTask<VectorCollectionPutCodec.RequestParameters> {

    VectorCollectionPutMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new PutOperation(parameters.name, parameters.key, parameters.value.getValue(), parameters.value.getVectors())
                .setPartitionId(getPartitionId());
    }

    @Override
    protected VectorCollectionPutCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return VectorCollectionPutCodec.decodeRequest(clientMessage);
    }

    @Override
    protected Object processResponseBeforeSending(Object response) {
        recordStats(LocalVectorCollectionStatsImpl::incrementPutLatencyNanos);
        return response;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return VectorCollectionPutCodec.encodeResponse(VectorUtil.serialize((VectorDocument) response, serializationService));
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.PUT;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.value};
    }

    @Override
    public Permission getRequiredPermission() {
        return new VectorCollectionPermission(getDistributedObjectName(), ACTION_PUT);
    }
}

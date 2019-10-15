/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheGetInvalidationMetaDataOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.security.Permission;

public class CacheFetchNearCacheInvalidationMetadataTask
        extends AbstractInvocationMessageTask<CacheFetchNearCacheInvalidationMetadataCodec.RequestParameters> {

    public CacheFetchNearCacheInvalidationMetadataTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(getServiceName(), op, parameters.address);
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheGetInvalidationMetaDataOperation(parameters.names);
    }

    @Override
    protected CacheFetchNearCacheInvalidationMetadataCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        parameters = CacheFetchNearCacheInvalidationMetadataCodec.decodeRequest(clientMessage);
        parameters.address = clientEngine.memberAddressOf(parameters.address);
        return parameters;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        CacheGetInvalidationMetaDataOperation.MetaDataResponse metaDataResponse =
                (CacheGetInvalidationMetaDataOperation.MetaDataResponse) response;
        return CacheFetchNearCacheInvalidationMetadataCodec
                .encodeResponse(metaDataResponse.getNamePartitionSequenceList().entrySet(),
                        metaDataResponse.getPartitionUuidList().entrySet());
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.names};
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
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
        return null;
    }
}

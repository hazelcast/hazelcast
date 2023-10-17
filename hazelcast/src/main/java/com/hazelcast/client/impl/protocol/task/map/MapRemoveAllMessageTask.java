/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.MapRemoveAllCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.security.Permission;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.map.impl.EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

public class MapRemoveAllMessageTask extends AbstractMapAllPartitionsMessageTask<MapRemoveAllCodec.RequestParameters> {

    private Predicate predicate;

    public MapRemoveAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        if (!(predicate instanceof PartitionPredicate)) {
            super.processMessage();
            return;
        }
        PartitionPredicate partitionPredicate = (PartitionPredicate) predicate;
        OperationFactory operationFactory = createOperationFactory();
        OperationServiceImpl operationService = nodeEngine.getOperationService();

        CompletableFuture<Map<Integer, Object>> future = operationService
            .invokeOnPartitionsAsync(
                getServiceName(),
                operationFactory,
                nodeEngine.getPartitionService().getPartitionIdSet(partitionPredicate.getPartitionKeys())
            );
        future.whenCompleteAsync((response, throwable) -> {
            if (throwable == null) {
                sendResponse(reduce(response));
            } else {
                handleProcessingFailure(throwable);
            }
        }, CALLER_RUNS);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        MapOperationProvider operationProvider = getOperationProvider(parameters.name);
        Predicate effectivePredicate = predicate instanceof PartitionPredicate ? ((PartitionPredicate) predicate)
                .getTarget() : predicate;
        return operationProvider.createPartitionWideEntryWithPredicateOperationFactory(parameters.name, ENTRY_REMOVING_PROCESSOR,
                effectivePredicate);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return null;
    }

    @Override
    protected MapRemoveAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        MapRemoveAllCodec.RequestParameters parameters = MapRemoveAllCodec.decodeRequest(clientMessage);
        predicate = serializationService.toObject(parameters.predicate);
        return parameters;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapRemoveAllCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.REMOVE_ALL;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.predicate};
    }
}

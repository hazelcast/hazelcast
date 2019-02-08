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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.operations.OperationFactoryWrapper;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapRemoveAllCodec;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;

import java.security.Permission;
import java.util.Collections;
import java.util.Map;

import static com.hazelcast.map.impl.EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static java.util.Collections.singletonList;

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

        int partitionId = getPartitionId();
        boolean invokedOnPartition = partitionId != -1;
        if (!invokedOnPartition) {
            // We got an old client which is sending -1 as a partition ID in its messages
            // while still using partition predicates.

            PartitionPredicate partitionPredicate = (PartitionPredicate) predicate;
            MapService mapService = getService(getServiceName());
            MapServiceContext mapServiceContext = mapService.getMapServiceContext();
            MapContainer mapContainer = mapServiceContext.getMapContainer(parameters.name);
            PartitioningStrategy partitioningStrategy = mapContainer.getPartitioningStrategy();
            Data partitionKey = serializationService.toData(partitionPredicate.getPartitionKey(), partitioningStrategy);
            partitionId = nodeEngine.getPartitionService().getPartitionId(partitionKey);
        }

        OperationFactory operationFactory = createOperationFactory();
        InternalOperationService operationService = nodeEngine.getOperationService();
        if (invokedOnPartition) {
            // We are running on a partition thread now and we are not allowed
            // to call invokeOnPartitions(Async) on operation service because of
            // that.

            Operation operation;
            if (operationFactory instanceof PartitionAwareOperationFactory) {
                // If operation factory is partition-aware, we should utilize this to our advantage
                // since for the on-heap storages this may speed up the operation via indexes
                // (see PartitionWideEntryWithPredicateOperationFactory.createFactoryOnRunner).

                PartitionAwareOperationFactory partitionAwareOperationFactory = (PartitionAwareOperationFactory) operationFactory;
                partitionAwareOperationFactory = partitionAwareOperationFactory
                        .createFactoryOnRunner(nodeEngine, new int[]{partitionId});
                operation = partitionAwareOperationFactory.createPartitionOperation(partitionId);
            } else {
                operation = operationFactory.createOperation();
            }

            final int thisPartitionId = partitionId;
            operation.setCallerUuid(endpoint.getUuid());
            ICompletableFuture<Object> future = operationService.invokeOnPartition(getServiceName(), operation, partitionId);
            future.andThen(new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    MapRemoveAllMessageTask.this.onResponse(Collections.singletonMap(thisPartitionId, response));
                }

                @Override
                public void onFailure(Throwable t) {
                    MapRemoveAllMessageTask.this.onFailure(t);
                }
            });
        } else {
            // invokeOnPartitionsAsync is used intentionally here, instead of asyncInvokeOnPartition,
            // since the later one doesn't support PartitionAwareOperationFactory, which we need to use
            // to speed up the removal operation using global indexes
            // (see PartitionWideEntryWithPredicateOperationFactory.createFactoryOnRunner).

            operationFactory = new OperationFactoryWrapper(operationFactory, endpoint.getUuid());
            ICompletableFuture<Map<Integer, Object>> future =
                    operationService.invokeOnPartitionsAsync(getServiceName(), operationFactory, singletonList(partitionId));
            future.andThen(this);
        }
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
        return "removeAll";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.predicate};
    }
}

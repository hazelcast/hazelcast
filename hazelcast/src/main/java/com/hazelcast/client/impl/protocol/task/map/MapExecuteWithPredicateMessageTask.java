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
import com.hazelcast.client.impl.protocol.codec.MapExecuteWithPredicateCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.client.impl.protocol.task.BlockingMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

public class MapExecuteWithPredicateMessageTask
        extends AbstractCallableMessageTask<MapExecuteWithPredicateCodec.RequestParameters> implements BlockingMessageTask {

    public MapExecuteWithPredicateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        OperationServiceImpl operationService = nodeEngine.getOperationService();

        Predicate predicate = serializationService.toObject(parameters.predicate);

        if (predicate instanceof PartitionPredicate) {
            return invokeOnPartition((PartitionPredicate) predicate, operationService);
        }
        OperationFactory operationFactory = new OperationFactoryWrapper(createOperationFactory(predicate), endpoint.getUuid());
        Map<Integer, Object> map = operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
        return reduce(map);
    }

    private Object invokeOnPartition(PartitionPredicate partitionPredicate,
                                     OperationServiceImpl operationService) {
        int partitionId = clientMessage.getPartitionId();
        Predicate predicate = partitionPredicate.getTarget();
        OperationFactory factory = createOperationFactory(predicate);
        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(getServiceName(),
                factory.createOperation(), partitionId);
        Object result = invocationBuilder.invoke().joinInternal();
        return reduce(Collections.singletonMap(partitionId, result));
    }

    private OperationFactory createOperationFactory(Predicate predicate) {
        MapOperationProvider operationProvider = getOperationProvider(parameters.name);
        EntryProcessor entryProcessor = serializationService.toObject(parameters.entryProcessor);
        return operationProvider.
                createPartitionWideEntryWithPredicateOperationFactory(parameters.name, entryProcessor, predicate);
    }

    private MapOperationProvider getOperationProvider(String mapName) {
        MapService mapService = getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapOperationProvider(mapName);
    }

    protected Object reduce(Map<Integer, Object> map) {
        List<Map.Entry<Data, Data>> dataMap = new ArrayList<Map.Entry<Data, Data>>();
        MapService mapService = getService(MapService.SERVICE_NAME);
        for (Object o : map.values()) {
            if (o != null) {
                MapEntries mapEntries = (MapEntries) mapService.getMapServiceContext().toObject(o);
                mapEntries.putAllToList(dataMap);
            }
        }
        return dataMap;
    }

    @Override
    protected MapExecuteWithPredicateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapExecuteWithPredicateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapExecuteWithPredicateCodec.encodeResponse((List<Map.Entry<Data, Data>>) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }


    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_PUT, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "executeOnEntries";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.entryProcessor, parameters.predicate};
    }
}

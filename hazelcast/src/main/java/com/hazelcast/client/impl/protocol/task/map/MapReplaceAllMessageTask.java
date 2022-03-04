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
import com.hazelcast.client.impl.protocol.codec.MapReplaceAllCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.impl.MapEntryReplacingEntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.query.Predicates;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.function.BiFunction;

public class MapReplaceAllMessageTask
        extends AbstractMapPartitionMessageTask<MapReplaceAllCodec.RequestParameters> {

    private BiFunction function;

    public MapReplaceAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        OperationFactory operationFactory = createOperationFactory();
        return operationFactory.createOperation();
    }

    protected OperationFactory createOperationFactory() {
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        return operationProvider.createPartitionWideEntryWithPredicateOperationFactory(parameters.name,
           new MapEntryReplacingEntryProcessor(function), Predicates.alwaysTrue());
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected MapReplaceAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        MapReplaceAllCodec.RequestParameters parameters =  MapReplaceAllCodec.decodeRequest(clientMessage);
        function = serializationService.toObject(parameters.function);
        return parameters;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapReplaceAllCodec.encodeResponse();
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(getDistributedObjectName(), ActionConstants.ACTION_PUT);
    }

    @Override
    public String getMethodName() {
        return "replaceAll";
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }
}

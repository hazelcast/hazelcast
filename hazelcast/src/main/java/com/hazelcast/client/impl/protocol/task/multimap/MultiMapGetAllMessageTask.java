/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.multimap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MultiMapGetAllCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.DataCollection;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.operations.GetAllOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MultiMapGetAllMessageTask
        extends AbstractPartitionMessageTask<MultiMapGetAllCodec.RequestParameters> {

    public MultiMapGetAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetAllOperation(parameters.name, parameters.keys);
    }

    @Override
    protected MultiMapGetAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapGetAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        List<Map.Entry<Data, Collection<Data>>> responseParam = new ArrayList<>();

        for (Map.Entry<Data, Data> entry : ((MapEntries) response).entries()) {
            Collection<Data> c = ((DataCollection) serializationService.toObject(entry.getValue())).getCollection();
            responseParam.add(new MapEntrySimple<>(entry.getKey(), c));
        }
        return MultiMapGetAllCodec.encodeResponse(responseParam);
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return "getAll";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.keys};
    }

}

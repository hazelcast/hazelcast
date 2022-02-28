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

package com.hazelcast.client.impl.protocol.task.multimap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MultiMapPutAllCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.DataCollection;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.operations.PutAllOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class MultiMapPutAllMessageTask
        extends AbstractPartitionMessageTask<MultiMapPutAllCodec.RequestParameters> {

    public MultiMapPutAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        MapEntries mapEntries = new MapEntries();
        for (Map.Entry<Data, List<Data>> e : parameters.entries) {
            mapEntries.add(e.getKey(), serializationService.toData(new DataCollection(e.getValue())));
        }
        return new PutAllOperation(parameters.name, mapEntries);
    }

    @Override
    protected MultiMapPutAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapPutAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapPutAllCodec.encodeResponse();
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
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getMethodName() {
        return "putAll";
    }

    @Override
    public Object[] getParameters() {
        //NB: copied from MapPutAllMessageTask
        Map<Data, Data> map = createHashMap(parameters.entries.size());
        for (Map.Entry<Data, List<Data>> entry : parameters.entries) {
            map.put(entry.getKey(), serializationService.toData(new DataCollection(entry.getValue())));
        }
        return new Object[]{map};
    }

}


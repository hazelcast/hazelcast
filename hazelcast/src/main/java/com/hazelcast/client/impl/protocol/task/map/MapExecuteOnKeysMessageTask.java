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
import com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeysCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMultiPartitionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.security.Permission;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
public class MapExecuteOnKeysMessageTask
        extends AbstractMultiPartitionMessageTask<MapExecuteOnKeysCodec.RequestParameters> {

    public MapExecuteOnKeysMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        EntryProcessor processor = serializationService.toObject(parameters.entryProcessor);
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        return operationProvider.createMultipleEntryOperationFactory(parameters.name,
                new HashSet<Data>(parameters.keys), processor);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        List<Map.Entry<Data, Data>> entries = new ArrayList<Map.Entry<Data, Data>>();

        MapService mapService = getService(MapService.SERVICE_NAME);
        for (Object o : map.values()) {
            if (o != null) {
                MapEntries mapEntries = (MapEntries) mapService.getMapServiceContext().toObject(o);
                mapEntries.putAllToList(entries);
            }
        }
        return entries;
    }

    @Override
    public PartitionIdSet getPartitions() {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        int partitions = partitionService.getPartitionCount();
        PartitionIdSet partitionIds = new PartitionIdSet(partitions);
        Iterator<Data> iterator = parameters.keys.iterator();
        int addedPartitions = 0;
        while (iterator.hasNext() && addedPartitions < partitions) {
            Data key = iterator.next();
            if (partitionIds.add(partitionService.getPartitionId(key))) {
                addedPartitions++;
            }
        }
        return partitionIds;
    }

    @Override
    protected MapExecuteOnKeysCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapExecuteOnKeysCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapExecuteOnKeysCodec.encodeResponse((List<Map.Entry<Data, Data>>) response);
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
        return "executeOnKeys";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.keys, parameters.entryProcessor};
    }
}

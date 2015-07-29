/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.Node;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MultipleEntryOperationFactory;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MapExecuteOnKeysMessageTask
        extends AbstractMultiPartitionMessageTask<MapExecuteOnKeysCodec.RequestParameters> {

    public MapExecuteOnKeysMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        EntryProcessor entryProcessor = serializationService.toObject(parameters.entryProcessor);
        return new MultipleEntryOperationFactory(parameters.name, parameters.keys, entryProcessor);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        Map<Data, Data> dataMap = new HashMap<Data, Data>();

        MapService mapService = getService(MapService.SERVICE_NAME);
        for (Object o : map.values()) {
            if (o != null) {
                MapEntrySet entrySet = (MapEntrySet) mapService.getMapServiceContext().toObject(o);
                Set<Map.Entry<Data, Data>> entries = entrySet.getEntrySet();
                for (Map.Entry<Data, Data> entry : entries) {
                    dataMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return dataMap.entrySet();
    }

    @Override
    public Collection<Integer> getPartitions() {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitions = partitionService.getPartitionCount();
        int capacity = Math.min(partitions, parameters.keys.size());
        Set<Integer> partitionIds = new HashSet<Integer>(capacity);
        Iterator<Data> iterator = parameters.keys.iterator();
        while (iterator.hasNext() && partitionIds.size() < partitions) {
            Data key = iterator.next();
            partitionIds.add(partitionService.getPartitionId(key));
        }
        return partitionIds;
    }

    @Override
    protected MapExecuteOnKeysCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapExecuteOnKeysCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapExecuteOnKeysCodec.encodeResponse((Set<Map.Entry<Data, Data>>) response);
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

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

package com.hazelcast.client.impl.protocol.task.replicatedmap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapEntrySetCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import java.security.Permission;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReplicatedMapEntrySetMessageTask
        extends AbstractCallableMessageTask<ReplicatedMapEntrySetCodec.RequestParameters> {

    public ReplicatedMapEntrySetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ReplicatedMapService service = getService(ReplicatedMapService.SERVICE_NAME);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(parameters.name);
        Set<Map.Entry<Object, ReplicatedRecord>> entrySet = new HashSet<Map.Entry<Object, ReplicatedRecord>>();
        for (ReplicatedRecordStore store : stores) {
            entrySet.addAll(store.entrySet(false));
        }
        HashMap<Data, Data> dataMap = new HashMap<Data, Data>(entrySet.size());
        for (Map.Entry<Object, ReplicatedRecord> entry : entrySet) {
            Data key = serializationService.toData(entry.getKey());
            Data value = serializationService.toData(entry.getValue().getValue());
            dataMap.put(key, value);
        }
        return dataMap.entrySet();
    }


    @Override
    protected ReplicatedMapEntrySetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapEntrySetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ReplicatedMapEntrySetCodec.encodeResponse((Set<Map.Entry<Data, Data>>) response);
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "entrySet";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}

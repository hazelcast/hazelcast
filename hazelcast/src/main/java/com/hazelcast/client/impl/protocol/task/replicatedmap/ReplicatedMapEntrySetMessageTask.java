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
import com.hazelcast.client.impl.protocol.parameters.DataEntryListResultParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapEntrySetParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicatedMapEntrySetMessageTask extends AbstractCallableMessageTask<ReplicatedMapEntrySetParameters> {

    public ReplicatedMapEntrySetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() throws Exception {
        ReplicatedMapService replicatedMapService = getService(getServiceName());
        final ReplicatedRecordStore recordStore = replicatedMapService.getReplicatedRecordStore(parameters.name, true);

        final Set<Map.Entry> entrySet = recordStore.entrySet();
        List<Data> keys = new ArrayList<Data>(entrySet.size());
        List<Data> values = new ArrayList<Data>(entrySet.size());

        for (Map.Entry entry : entrySet) {
            keys.add(serializationService.toData(entry.getKey()));
            values.add(serializationService.toData(entry.getValue()));
        }

        return DataEntryListResultParameters.encode(keys, values);
    }


    @Override
    protected ReplicatedMapEntrySetParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapEntrySetParameters.decode(clientMessage);
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

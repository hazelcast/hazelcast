/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.MultiMapEntrySetCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.operations.EntrySetResponse;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.MultiMapMessageType#MULTIMAP_ENTRYSET}
 */
public class MultiMapEntrySetMessageTask
        extends AbstractAllPartitionsMessageTask<MultiMapEntrySetCodec.RequestParameters> {

    public MultiMapEntrySetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(parameters.name, MultiMapOperationFactory.OperationFactoryType.ENTRY_SET);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        List<Map.Entry<Data, Data>> entries = new ArrayList<Map.Entry<Data, Data>>();
        for (Object obj : map.values()) {
            if (obj == null) {
                continue;
            }
            EntrySetResponse response = (EntrySetResponse) obj;
            Set<Map.Entry<Data, Data>> entrySet = response.getDataEntrySet();
            for (Map.Entry<Data, Data> entry : entrySet) {
                entries.add(entry);
            }
        }
        return entries;
    }

    @Override
    protected MultiMapEntrySetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapEntrySetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapEntrySetCodec.encodeResponse((List<Map.Entry<Data, Data>>) response);
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_READ);
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

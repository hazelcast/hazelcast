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
import com.hazelcast.client.impl.protocol.codec.MultiMapKeySetCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.MultiMapMessageType#MULTIMAP_KEYSET}
 */
public class MultiMapKeySetMessageTask
        extends AbstractAllPartitionsMessageTask<MultiMapKeySetCodec.RequestParameters> {

    public MultiMapKeySetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(parameters.name, MultiMapOperationFactory.OperationFactoryType.KEY_SET);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        List<Data> keys = new ArrayList<Data>();

        for (Object obj : map.values()) {
            if (obj == null) {
                continue;
            }
            MultiMapResponse response = (MultiMapResponse) obj;
            Collection<Data> coll = response.getCollection();
            if (coll != null) {
                keys.addAll(coll);
            }
        }
        return keys;
    }

    @Override
    protected MultiMapKeySetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapKeySetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapKeySetCodec.encodeResponse((List<Data>) response);
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
        return "keySet";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}

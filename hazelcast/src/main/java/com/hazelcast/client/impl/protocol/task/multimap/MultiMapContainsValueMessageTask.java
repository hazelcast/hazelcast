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
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsValueCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.MultiMapMessageType#MULTIMAP_CONTAINSVALUE}
 */
public class MultiMapContainsValueMessageTask
        extends AbstractMultiMapAllPartitionsMessageTask<MultiMapContainsValueCodec.RequestParameters> {

    public MultiMapContainsValueMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(parameters.name,
                MultiMapOperationFactory.OperationFactoryType.CONTAINS, null, parameters.value);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        boolean found = false;
        for (Object obj : map.values()) {
            if (Boolean.TRUE.equals(obj)) {
                found = true;
                break;
            }
        }
        updateStats(LocalMapStatsImpl::incrementOtherOperations);
        return found;
    }

    @Override
    protected MultiMapContainsValueCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapContainsValueCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapContainsValueCodec.encodeResponse((Boolean) response);
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
        return "containsValue";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.value};
    }

}

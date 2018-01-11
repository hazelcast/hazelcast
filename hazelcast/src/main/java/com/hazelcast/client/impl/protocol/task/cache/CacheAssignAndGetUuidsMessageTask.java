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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAssignAndGetUuidsCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;

public class CacheAssignAndGetUuidsMessageTask
        extends AbstractCacheAllPartitionsTask<CacheAssignAndGetUuidsCodec.RequestParameters> {

    public CacheAssignAndGetUuidsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CacheAssignAndGetUuidsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAssignAndGetUuidsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAssignAndGetUuidsCodec.encodeResponse(((Map<Integer, UUID>) response).entrySet());
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new CacheAssignAndGetUuidsOperationFactory();
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return map;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}

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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractRemoveListenerMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Client request which unregisters the event listener on behalf of the client.
 *
 * @see com.hazelcast.cache.impl.CacheService#deregisterListener(String, UUID)
 */
public class CacheRemoveEntryListenerMessageTask
        extends AbstractRemoveListenerMessageTask<CacheRemoveEntryListenerCodec.RequestParameters> {

    public CacheRemoveEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected UUID getRegistrationId() {
        return parameters.registrationId;
    }

    @Override
    protected Future<Boolean> deRegisterListener() {
        CacheService service = getService(CacheService.SERVICE_NAME);
        return service.deregisterListenerAsync(parameters.name, parameters.registrationId);
    }

    @Override
    protected CacheRemoveEntryListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheRemoveEntryListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheRemoveEntryListenerCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return "deregisterCacheEntryListener";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}

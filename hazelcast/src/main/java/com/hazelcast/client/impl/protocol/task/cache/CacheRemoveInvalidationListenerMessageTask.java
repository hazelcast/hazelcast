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

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractRemoveListenerMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;

/**
 * Client request which unregisters the invalidation listener on behalf of the client.
 *
 * @see com.hazelcast.cache.impl.CacheService#deregisterListener(String, String)
 */
public class CacheRemoveInvalidationListenerMessageTask
        extends AbstractRemoveListenerMessageTask<CacheRemoveInvalidationListenerCodec.RequestParameters> {

    public CacheRemoveInvalidationListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected boolean deRegisterListener() {
        CacheService service = getService(CacheService.SERVICE_NAME);
        return service.deregisterListener(parameters.name, parameters.registrationId);
    }

    @Override
    protected String getRegistrationId() {
        return parameters.registrationId;
    }

    @Override
    protected CacheRemoveInvalidationListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheRemoveInvalidationListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheRemoveInvalidationListenerCodec.encodeResponse((Boolean) response);
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
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

}

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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.client.CacheInvalidationMessage;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;

public class CacheAddInvalidationListenerTask
        extends AbstractCallableMessageTask<CacheAddInvalidationListenerCodec.RequestParameters> {

    public CacheAddInvalidationListenerTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        final ClientEndpoint endpoint = getEndpoint();
        CacheService cacheService = getService(CacheService.SERVICE_NAME);
        String registrationId = cacheService.addInvalidationListener(parameters.name, new CacheEventListener() {
            @Override
            public void handleEvent(Object eventObject) {
                if (!endpoint.isAlive()) {
                    return;
                }
                if (eventObject instanceof CacheInvalidationMessage) {
                    CacheInvalidationMessage message = (CacheInvalidationMessage) eventObject;

                    ClientMessage eventMessage
                            = CacheAddInvalidationListenerCodec.encodeCacheInvalidationEvent(message.getName(),
                            message.getKey(), message.getSourceUuid());
                    sendClientMessage(message.getKey(), eventMessage);
                }
            }
        });
        endpoint.setListenerRegistration(CacheService.SERVICE_NAME, parameters.name, registrationId);
        return registrationId;
    }

    @Override
    protected CacheAddInvalidationListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAddInvalidationListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAddInvalidationListenerCodec.encodeResponse((String) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

}

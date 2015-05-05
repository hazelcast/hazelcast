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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AddDistributedObjectListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.AddListenerResultParameters;
import com.hazelcast.client.impl.protocol.parameters.DistributedObjectEventParameters;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.ProxyService;

import java.security.Permission;

public class AddDistributedObjectListenerMessageTask
        extends AbstractCallableMessageTask<AddDistributedObjectListenerParameters>
        implements DistributedObjectListener {

    public AddDistributedObjectListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() throws Exception {
        ProxyService proxyService = clientEngine.getProxyService();
        String registrationId = proxyService.addProxyListener(this);
        endpoint.setDistributedObjectListener(registrationId);
        return AddListenerResultParameters.encode(registrationId);
    }

    @Override
    protected AddDistributedObjectListenerParameters decodeClientMessage(ClientMessage clientMessage) {
        return AddDistributedObjectListenerParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
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
    public void distributedObjectCreated(DistributedObjectEvent event) {
        send(event);
    }

    @Override
    public void distributedObjectDestroyed(DistributedObjectEvent event) {
        send(event);
    }

    private void send(DistributedObjectEvent event) {
        if (endpoint.isAlive()) {
            String name = event.getDistributedObject().getName();
            String serviceName = event.getServiceName();
            ClientMessage eventMessage =
                    DistributedObjectEventParameters.encode(name, serviceName, event.getEventType());
            sendClientMessage(null, eventMessage);
        }
    }
}

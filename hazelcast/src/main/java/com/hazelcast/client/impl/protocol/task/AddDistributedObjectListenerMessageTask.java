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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;

import java.security.Permission;
import java.util.concurrent.Callable;

public class AddDistributedObjectListenerMessageTask
        extends AbstractCallableMessageTask<ClientAddDistributedObjectListenerCodec.RequestParameters>
        implements DistributedObjectListener {

    public AddDistributedObjectListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        final ProxyService proxyService = clientEngine.getProxyService();
        final String registrationId = proxyService.addProxyListener(this);
        endpoint.addDestroyAction(registrationId, new Callable() {
            @Override
            public Boolean call() {
                return proxyService.removeProxyListener(registrationId);
            }
        });

        return registrationId;
    }

    @Override
    protected ClientAddDistributedObjectListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientAddDistributedObjectListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientAddDistributedObjectListenerCodec.encodeResponse((String) response);
    }

    @Override
    public String getServiceName() {
        return ProxyServiceImpl.SERVICE_NAME;
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
        return "addDistributedObjectListener";
    }

    @Override
    public Object[] getParameters() {
        return null;
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
        if (!shouldSendEvent()) {
            return;
        }

        String name = (String) event.getObjectName();
        String serviceName = event.getServiceName();
        ClientMessage eventMessage =
                ClientAddDistributedObjectListenerCodec.encodeDistributedObjectEvent(name,
                        serviceName, event.getEventType().name());
        sendClientMessage(null, eventMessage);
    }

    private boolean shouldSendEvent() {
        if (!endpoint.isAlive()) {
            return false;
        }

        ClusterService clusterService = clientEngine.getClusterService();
        boolean currentMemberIsMaster = clusterService.getMasterAddress().equals(clientEngine.getThisAddress());
        if (parameters.localOnly && !currentMemberIsMaster) {
            //if client registered localOnly, only master is allowed to send request
            return false;
        }
        return true;
    }
}

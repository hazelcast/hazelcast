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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;

import java.security.Permission;
import java.util.UUID;

public class RemoveDistributedObjectListenerMessageTask
        extends AbstractCallableMessageTask<UUID> {

    public RemoveDistributedObjectListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call()
            throws Exception {
        endpoint.removeDestroyAction(parameters);
        return clientEngine.getProxyService().removeProxyListener(parameters);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }

    @Override
    protected UUID decodeClientMessage(ClientMessage clientMessage) {
        return ClientRemoveDistributedObjectListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientRemoveDistributedObjectListenerCodec.encodeResponse((Boolean) response);
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
        return "removeDistributedObjectListener";
    }

}

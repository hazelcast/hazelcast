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

import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;

import java.security.Permission;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class GetDistributedObjectsMessageTask
        extends AbstractCallableMessageTask<ClientGetDistributedObjectsCodec.RequestParameters> {

    public GetDistributedObjectsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call()
            throws Exception {
        Collection<DistributedObject> distributedObjects = clientEngine.getProxyService().getAllDistributedObjects();

        Set<DistributedObjectInfo> coll = new HashSet<DistributedObjectInfo>(distributedObjects.size());
        for (DistributedObject distributedObject : distributedObjects) {
            coll.add(new DistributedObjectInfo(distributedObject.getServiceName(), distributedObject.getName()));
        }
        return coll;
    }

    @Override
    protected ClientGetDistributedObjectsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientGetDistributedObjectsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientGetDistributedObjectsCodec.encodeResponse((Set<DistributedObjectInfo>) response);
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
        return "getDistributedObjects";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}

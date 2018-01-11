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

import com.hazelcast.client.impl.ClientPartitionListenerService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddPartitionListenerCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.UuidUtil;

import java.security.Permission;
import java.util.concurrent.Callable;

public class AddPartitionListenerMessageTask
        extends AbstractCallableMessageTask<ClientAddPartitionListenerCodec.RequestParameters> {

    public AddPartitionListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        InternalPartitionService internalPartitionService = getService(InternalPartitionService.SERVICE_NAME);
        internalPartitionService.firstArrangement();
        final ClientPartitionListenerService service = clientEngine.getPartitionListenerService();
        service.registerPartitionListener(endpoint, clientMessage.getCorrelationId());
        endpoint.addDestroyAction(UuidUtil.newUnsecureUUID().toString(), new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                service.deregisterPartitionListener(endpoint);
                return Boolean.TRUE;
            }
        });
        return true;
    }

    @Override
    protected ClientAddPartitionListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientAddPartitionListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientAddPartitionListenerCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
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
        return null;
    }

    public Permission getRequiredPermission() {
        return null;
    }

}

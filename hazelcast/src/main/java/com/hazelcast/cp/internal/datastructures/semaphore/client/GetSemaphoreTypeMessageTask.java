/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.semaphore.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreGetSemaphoreTypeCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;

/**
 * Client message task for querying semaphore JDK compatibility
 */
public class GetSemaphoreTypeMessageTask extends AbstractMessageTask<CPSemaphoreGetSemaphoreTypeCodec.RequestParameters> {

    public GetSemaphoreTypeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        CPSemaphoreConfig config = nodeEngine.getConfig().getCPSubsystemConfig().findSemaphoreConfig(parameters.proxyName);
        boolean jdkCompatible = (config != null && config.isJDKCompatible());
        sendResponse(jdkCompatible);
    }

    @Override
    protected CPSemaphoreGetSemaphoreTypeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPSemaphoreGetSemaphoreTypeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSemaphoreGetSemaphoreTypeCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.proxyName;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

}

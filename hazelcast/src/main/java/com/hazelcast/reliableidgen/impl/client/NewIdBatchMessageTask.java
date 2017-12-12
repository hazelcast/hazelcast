/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.reliableidgen.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ReliableIdGeneratorNewIdBatchCodec;
import com.hazelcast.client.impl.protocol.codec.ReliableIdGeneratorNewIdBatchCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.reliableidgen.impl.ReliableIdGeneratorService;
import com.hazelcast.reliableidgen.impl.IdBatch;
import com.hazelcast.reliableidgen.impl.ReliableIdGeneratorProxy;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReliableIdGeneratorPermission;

import java.security.Permission;

public class NewIdBatchMessageTask
        extends AbstractCallableMessageTask<RequestParameters> {

    public NewIdBatchMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ReliableIdGeneratorNewIdBatchCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReliableIdGeneratorNewIdBatchCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        IdBatch idBatch = (IdBatch) response;
        return ReliableIdGeneratorNewIdBatchCodec.encodeResponse(idBatch.base(), idBatch.increment(), idBatch.batchSize());
    }

    @Override
    protected IdBatch call() throws Exception {
        ReliableIdGeneratorProxy proxy = (ReliableIdGeneratorProxy) nodeEngine.getProxyService()
                .getDistributedObject(getServiceName(), parameters.name);
        return proxy.newIdBatch(parameters.batchSize);
    }

    @Override
    public String getServiceName() {
        return ReliableIdGeneratorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReliableIdGeneratorPermission(parameters.name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "newIdBatch";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}

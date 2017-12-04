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

package com.hazelcast.client.impl.protocol.task.flakeidgenerator;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.concurrent.flakeidgen.FlakeIdGeneratorProxy;
import com.hazelcast.concurrent.flakeidgen.FlakeIdGeneratorService;
import com.hazelcast.concurrent.flakeidgen.IdBatch;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.FlakeIdGeneratorPermission;

import java.security.Permission;

public class NewIdBatchMessageTask
        extends AbstractCallableMessageTask<RequestParameters> {

    public NewIdBatchMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected FlakeIdGeneratorNewIdBatchCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return FlakeIdGeneratorNewIdBatchCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        IdBatch idBatch = (IdBatch) response;
        return FlakeIdGeneratorNewIdBatchCodec.encodeResponse(idBatch.base(), idBatch.increment(), idBatch.batchSize());
    }

    @Override
    protected IdBatch call() throws Exception {
        FlakeIdGeneratorProxy proxy = (FlakeIdGeneratorProxy) nodeEngine.getProxyService()
                .getDistributedObject(getServiceName(), parameters.name);
        return proxy.newIdBatch(parameters.batchSize);
    }

    @Override
    public String getServiceName() {
        return FlakeIdGeneratorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new FlakeIdGeneratorPermission(parameters.name, ActionConstants.ACTION_MODIFY);
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
